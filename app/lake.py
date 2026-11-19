"""Core data-lake logic: uploads, consistent reads, background compaction.

Consistency model (single process, SQLite as the source of truth):

* Upload: parquet is written to a tmp file, fsynced, atomically renamed,
  then registered ``active`` in SQLite.  A read only ever sees fully
  registered files.
* Read: the active file list is snapshotted and refcounted under one lock,
  so a read always corresponds to the complete pre- or post-switch state --
  never a mix.  Retired files stay on disk until their refcount hits zero.
* Compaction: merges the files registered at job start into one parquet,
  then switches the registry in a single transaction.  Rows appended during
  the merge are not part of the input snapshot, so they survive the switch
  untouched and remain readable throughout.
* Recovery: the switch and the job-status update commit atomically, so a
  job still ``running`` at startup is guaranteed not to have switched; it
  is simply re-executed.  Orphaned bytes (failed uploads, superseded
  compaction outputs, tmp fragments) are swept before serving traffic.
* Withdrawal: uploads made while the feature is enabled become tracked
  batches whose row multiset is persisted alongside parquet provenance.
  Withdrawing a batch rewrites (multiset difference) the file currently
  carrying its rows -- the original name stays a valid handle even after
  compaction and garbage collection.  The registry switch and the batch's
  ``withdrawn`` state commit in one transaction, so a failed write keeps
  every original row and the batch stays withdrawable.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from .batch import (
    multiset_difference,
    read_parquet_rows,
    rows_to_table,
    table_to_rows,
)
from .csvio import CsvError, empty_table, parse_csv
from .db import connect, init_schema
from .storage import write_parquet_atomic

log = logging.getLogger("buoy-lake")


class ConflictError(RuntimeError):
    """A compaction is already running for this day."""


class WithdrawalConflictError(RuntimeError):
    """The target file changed while the withdrawal was being applied."""


class WithdrawalNotAllowedError(RuntimeError):
    """The file cannot be withdrawn (unknown name / feature disabled)."""


class WithdrawalWriteError(RuntimeError):
    """Writing the withdrawal rewrite failed; original data is untouched."""


class NotFoundError(KeyError):
    """Requested object does not exist."""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class Lake:
    def __init__(
        self,
        data_dir: str | os.PathLike,
        compaction_delay_sec: float = 0.0,
        withdrawals_enabled: bool = True,
    ):
        self.data_dir = Path(data_dir)
        self.db_path = self.data_dir / "lake_meta.db"
        # Test/demo hook: pause after the input snapshot so callers can
        # observe uploads and reads happening *while* a compaction runs.
        self.compaction_delay_sec = compaction_delay_sec
        # When False, new uploads are not tracked for withdrawal; batches
        # already tracked keep being withdrawable.
        self.withdrawals_enabled = withdrawals_enabled
        # Guards the inflight refcounts and serialises
        # (snapshot + acquire) against (retired-file reclamation).
        self._state_lock = threading.Lock()
        self._inflight: dict[str, int] = {}
        self.data_dir.mkdir(parents=True, exist_ok=True)
        init_schema(str(self.db_path))

    # ------------------------------------------------------------------
    # paths
    # ------------------------------------------------------------------
    def _day_dir(self, day: str) -> Path:
        return self.data_dir / f"dt={day}"

    # ------------------------------------------------------------------
    # uploads
    # ------------------------------------------------------------------
    def upload(self, day: str, content: bytes) -> dict:
        """Parse CSV, write one parquet file, register it. Raises CsvError."""
        table = parse_csv(content)
        n = table.num_rows
        day_dir = self._day_dir(day)
        day_dir.mkdir(parents=True, exist_ok=True)
        final = day_dir / f"part-{uuid4().hex}.parquet"
        write_parquet_atomic(table, final)
        now = _now_iso()
        try:
            with connect(str(self.db_path)) as conn:
                cur = conn.execute(
                    "INSERT INTO files(day, path, kind, state, rows, created_at)"
                    " VALUES (?, ?, 'upload', 'active', ?, ?)",
                    (day, str(final), n, now),
                )
                tracked = False
                if self.withdrawals_enabled:
                    # The batch's full row multiset is persisted so it stays
                    # withdrawable even after this parquet is compacted away
                    # and its bytes are garbage collected.
                    conn.execute(
                        "INSERT INTO batches(day, file_name, file_path, rows, state, created_at)"
                        " VALUES (?, ?, ?, ?, 'active', ?)",
                        (day, final.name, str(final), n, now),
                    )
                    batch_id = conn.execute(
                        "SELECT id FROM batches WHERE file_name = ?", (final.name,)
                    ).fetchone()["id"]
                    conn.executemany(
                        "INSERT INTO batch_rows(batch_id, buoy_id, ts_us, salinity)"
                        " VALUES (?, ?, ?, ?)",
                        [(batch_id, b, ts, sal) for (b, ts, sal) in table_to_rows(table)],
                    )
                    tracked = True
        except Exception:
            final.unlink(missing_ok=True)  # never registered -> remove bytes
            raise
        log.info("uploaded %s rows to %s", n, final.name)
        return {
            "day": day,
            "file": final.name,
            "rows": n,
            "file_count": self.file_count(day),
            "withdrawable": tracked,
        }

    # ------------------------------------------------------------------
    # reads
    # ------------------------------------------------------------------
    def _snapshot_active(self, day: str) -> list[str]:
        """Active files for ``day`` + inflight ref, atomic w.r.t. the reclaimer."""
        with self._state_lock:
            with connect(str(self.db_path)) as conn:
                rows = conn.execute(
                    "SELECT path FROM files WHERE day = ? AND state = 'active' ORDER BY id",
                    (day,),
                ).fetchall()
            paths = [r["path"] for r in rows]
            for p in paths:
                self._inflight[p] = self._inflight.get(p, 0) + 1
            return paths

    def _release(self, paths: list[str]) -> None:
        with self._state_lock:
            for p in paths:
                remaining = self._inflight.get(p, 0) - 1
                if remaining <= 0:
                    self._inflight.pop(p, None)
                else:
                    self._inflight[p] = remaining
        self.collect_garbage()

    def read_day(self, day: str) -> dict:
        """All records for the day from one consistent (pre/post-switch) snapshot."""
        paths = self._snapshot_active(day)
        try:
            tables = [pq.read_table(p) for p in paths]
            merged = pa.concat_tables(tables, promote_options="none") if tables else empty_table()
            cols = merged.to_pydict()
            records = [
                {"buoy_id": b, "ts_utc": _iso_z(t), "salinity": s}
                for b, t, s in zip(cols["buoy_id"], cols["ts_utc"], cols["salinity"])
            ]
            return {
                "day": day,
                "file_count": len(paths),
                "row_count": merged.num_rows,
                "records": records,
            }
        finally:
            self._release(paths)

    def file_count(self, day: str) -> int:
        with connect(str(self.db_path)) as conn:
            (n,) = conn.execute(
                "SELECT COUNT(*) FROM files WHERE day = ? AND state = 'active'", (day,)
            ).fetchone()
        return n

    def list_files(self, day: str) -> dict:
        with connect(str(self.db_path)) as conn:
            rows = conn.execute(
                "SELECT path, kind, rows, created_at FROM files"
                " WHERE day = ? AND state = 'active' ORDER BY id",
                (day,),
            ).fetchall()
        return {
            "day": day,
            "file_count": len(rows),
            "files": [
                {
                    "file": Path(r["path"]).name,
                    "kind": r["kind"],
                    "rows": r["rows"],
                    "created_at": r["created_at"],
                }
                for r in rows
            ],
        }

    # ------------------------------------------------------------------
    # compaction
    # ------------------------------------------------------------------
    def start_compaction(self, day: str) -> dict:
        """Register and launch a background compaction. Raises ConflictError."""
        input_rows = 0
        with connect(str(self.db_path)) as conn:
            rows = conn.execute(
                "SELECT path, rows FROM files WHERE day = ? AND state = 'active' ORDER BY id",
                (day,),
            ).fetchall()
            input_paths = [r["path"] for r in rows]
            input_rows = sum(r["rows"] for r in rows)
            try:
                cur = conn.execute(
                    "INSERT INTO compactions(day, status, input_paths, input_rows, created_at)"
                    " VALUES (?, 'running', ?, ?, ?)",
                    (day, json.dumps(input_paths), input_rows, _now_iso()),
                )
            except sqlite3.IntegrityError as exc:
                raise ConflictError(f"a compaction is already running for day {day}") from exc
            job_id = cur.lastrowid
        self._launch_worker(job_id, day, input_paths)
        log.info("compaction job %s started for %s over %d files", job_id, day, len(input_paths))
        return self.get_compaction(job_id)

    def _launch_worker(self, job_id: int, day: str, input_paths: list[str]) -> None:
        t = threading.Thread(
            target=self._run_compaction,
            args=(job_id, day, input_paths),
            name=f"compact-{day}-{job_id}",
            daemon=True,
        )
        t.start()

    def _run_compaction(self, job_id: int, day: str, input_paths: list[str]) -> None:
        output_path: Path | None = None
        try:
            if self.compaction_delay_sec > 0:
                time.sleep(self.compaction_delay_sec)

            if input_paths:
                tables = [pq.read_table(p) for p in input_paths]
                merged = pa.concat_tables(tables, promote_options="none")
            else:
                merged = empty_table()

            if input_paths:
                day_dir = self._day_dir(day)
                day_dir.mkdir(parents=True, exist_ok=True)
                output_path = day_dir / f"compacted-{job_id}-{uuid4().hex[:8]}.parquet"
                # Fully written + fsynced *before* the registry switch.
                write_parquet_atomic(merged, output_path)

            # Atomic switch: retire inputs, register output, record lineage,
            # mark job done.  Everything commits together (or nothing does).
            conn = connect(str(self.db_path))
            try:
                conn.execute("BEGIN IMMEDIATE")
                if input_paths:
                    placeholders = ",".join("?" for _ in input_paths)
                    # A late compaction must never write back rows that were
                    # withdrawn while it was merging: if any snapshot input is
                    # no longer the active file it used to be, abort and fail
                    # the job; all original data stays registered and readable.
                    stale = conn.execute(
                        f"SELECT COUNT(*) FROM files"
                        f" WHERE path IN ({placeholders})"
                        f"   AND NOT (day = ? AND state = 'active')",
                        [*input_paths, day],
                    ).fetchone()[0]
                    if stale:
                        raise WithdrawalConflictError(
                            f"compaction job {job_id}: {stale} input file(s) changed"
                            " (withdrawn) since the job started; refusing to switch"
                        )
                    conn.execute(
                        f"UPDATE files SET state = 'retired'"
                        f" WHERE day = ? AND state = 'active' AND path IN ({placeholders})",
                        [day, *input_paths],
                    )
                    conn.execute(
                        "INSERT INTO files(day, path, kind, state, rows, created_at)"
                        " VALUES (?, ?, 'compacted', 'active', ?, ?)",
                        (day, str(output_path), merged.num_rows, _now_iso()),
                    )
                    conn.executemany(
                        "INSERT INTO file_lineage(src_path, dst_path, op)"
                        " VALUES (?, ?, 'compaction')",
                        [(p, str(output_path)) for p in input_paths],
                    )
                conn.execute(
                    "UPDATE compactions SET status = 'done', output_path = ?,"
                    " output_rows = ?, finished_at = ? WHERE id = ?",
                    (str(output_path) if output_path else None, merged.num_rows, _now_iso(), job_id),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()
            log.info("compaction job %s done: %d files -> %d rows", job_id, len(input_paths), merged.num_rows)
        except Exception as exc:  # originals stay registered; nothing is lost
            log.exception("compaction job %s failed", job_id)
            if output_path is not None:
                output_path.unlink(missing_ok=True)
            try:
                with connect(str(self.db_path)) as conn:
                    conn.execute(
                        "UPDATE compactions SET status = 'failed', error = ?, finished_at = ?"
                        " WHERE id = ? AND status = 'running'",
                        (f"{type(exc).__name__}: {exc}", _now_iso(), job_id),
                    )
            except Exception:
                log.exception("could not mark compaction job %s failed", job_id)
        finally:
            try:
                self.collect_garbage()
            except Exception:
                log.exception("garbage collection after job %s failed", job_id)

    # ------------------------------------------------------------------
    # compaction status
    # ------------------------------------------------------------------
    @staticmethod
    def _job_dict(row: sqlite3.Row) -> dict:
        return {
            "job_id": row["id"],
            "day": row["day"],
            "status": row["status"],
            "input_files": [Path(p).name for p in json.loads(row["input_paths"])],
            "input_rows": row["input_rows"],
            "output_file": Path(row["output_path"]).name if row["output_path"] else None,
            "output_rows": row["output_rows"],
            "error": row["error"],
            "created_at": row["created_at"],
            "finished_at": row["finished_at"],
        }

    def get_compaction(self, job_id: int) -> dict:
        with connect(str(self.db_path)) as conn:
            row = conn.execute("SELECT * FROM compactions WHERE id = ?", (job_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"compaction job {job_id} not found")
        return self._job_dict(row)

    def list_compactions(self, day: str) -> list[dict]:
        with connect(str(self.db_path)) as conn:
            rows = conn.execute(
                "SELECT * FROM compactions WHERE day = ? ORDER BY id DESC", (day,)
            ).fetchall()
        return [self._job_dict(r) for r in rows]

    # ------------------------------------------------------------------
    # withdrawals
    # ------------------------------------------------------------------
    def _resolve_active_file(self, start_path: str) -> str | None:
        """Walk parquet provenance edges to the file currently carrying rows.

        Returns the active registry path reached from ``start_path``, or
        ``None`` if the chain ends at a retired file that has been garbage
        collected (should not happen while a tracked batch is still active).
        """
        with connect(str(self.db_path)) as conn:
            current = start_path
            for _ in range(10_000):  # defensive: lineage chains are short
                rows = conn.execute(
                    "SELECT dst_path FROM file_lineage WHERE src_path = ? ORDER BY rowid",
                    (current,),
                ).fetchall()
                if not rows:
                    break
                nxt = rows[-1]["dst_path"]
                if nxt == "":
                    # Terminal edge: a withdrawal removed the file's last rows,
                    # so there is no successor parquet.
                    return None
                current = nxt
            reg = conn.execute(
                "SELECT state FROM files WHERE path = ?", (current,)
            ).fetchone()
        if reg is None or reg["state"] != "active":
            return None
        return current

    def _batch_rows(self, conn: sqlite3.Connection, batch_id: int) -> list:
        rows = conn.execute(
            "SELECT buoy_id, ts_us, salinity FROM batch_rows WHERE batch_id = ?",
            (batch_id,),
        ).fetchall()
        return [(r["buoy_id"], r["ts_us"], r["salinity"]) for r in rows]

    def withdraw(self, day: str, file_name: str) -> dict:
        """Withdraw the batch identified by its upload-response file name.

        Multiset semantics: every row of the batch (including its internal
        duplicates) is removed from whichever active parquet currently carries
        it; identical values from other batches and unattributed legacy rows
        stay.  Idempotent -- withdrawing an already withdrawn batch reports its
        existing withdrawal instead of touching any data.
        """
        with connect(str(self.db_path)) as conn:
            batch = conn.execute(
                "SELECT * FROM batches WHERE day = ? AND file_name = ?",
                (day, file_name),
            ).fetchone()
            if batch is None:
                # Distinguish "unknown name" from "upload not tracked".
                legacy = conn.execute(
                    "SELECT 1 FROM files WHERE day = ? AND"
                    " (path LIKE ? OR path = ?) LIMIT 1",
                    (day, f"%/{file_name}", file_name),
                ).fetchone()
                if legacy is not None:
                    raise WithdrawalNotAllowedError(
                        f"file {file_name!r} was uploaded before withdrawals were"
                        " enabled and cannot be withdrawn"
                    )
                raise NotFoundError(f"no tracked upload named {file_name!r} on day {day}")
            batch_id = batch["id"]
            if batch["state"] == "withdrawn":
                wd = conn.execute(
                    "SELECT * FROM withdrawals WHERE batch_id = ?", (batch_id,)
                ).fetchone()
                return self._withdrawal_dict(conn, wd, already=True)
            batch_rows = self._batch_rows(conn, batch_id)

        # Resolve the active file through compaction / rewrite provenance.
        target_path = self._resolve_active_file(batch["file_path"])
        if target_path is None:
            raise NotFoundError(
                f"current parquet for batch {file_name!r} is no longer registered"
            )

        # Compute the multiset difference BEFORE opening the switch
        # transaction so the expensive parquet work never holds a write lock.
        def _compute(path: str):
            current = read_parquet_rows(path)
            kept = multiset_difference(current, batch_rows)
            if len(kept) == len(current):
                # Invariant broken: none of the batch's rows are in its
                # lineage file.  Refuse rather than silently no-op.
                raise WithdrawalConflictError(
                    f"batch {file_name!r} rows are not present in the current file"
                )
            return current, kept

        day_dir = self._day_dir(day)
        day_dir.mkdir(parents=True, exist_ok=True)
        rewrite_name = f"rewrite-{batch_id}-{uuid4().hex[:8]}.parquet"
        rewrite_path = day_dir / rewrite_name

        def _materialize(path: str):
            """Resolve, compute the difference, and stage the rewrite bytes.

            Raises WithdrawalWriteError when the parquet write fails: the
            registry has not been touched, so the original data and the
            batch's active state are fully preserved and the call is retryable.
            """
            current, kept = _compute(path)
            wrote = False
            if kept:
                try:
                    write_parquet_atomic(rows_to_table(kept), rewrite_path)
                    wrote = True
                except Exception as exc:  # storage cleans its own tmp fragment
                    raise WithdrawalWriteError(
                        f"could not write withdrawal rewrite for {file_name!r}: {exc}"
                    ) from exc
            else:
                rewrite_path.unlink(missing_ok=True)
            return current, kept, wrote

        current_rows, kept_rows, wrote_rewrite = _materialize(target_path)

        last_exc: Exception | None = None
        for attempt in range(3):
            try:
                return self._withdrawal_switch(
                    day=day,
                    batch_id=batch_id,
                    file_name=file_name,
                    target_path=target_path,
                    expected_rows=len(current_rows),
                    rewrite_path=rewrite_path,
                    kept_rows=kept_rows,
                    wrote_rewrite=wrote_rewrite,
                )
            except WithdrawalConflictError as exc:
                # Another withdrawal/compaction changed the file under us:
                # re-resolve and retry with a fresh multiset difference.
                last_exc = exc
                log.warning("withdrawal conflict for %s (attempt %d): %s",
                            file_name, attempt + 1, exc)
                target_path = self._resolve_active_file(batch["file_path"])
                if target_path is None:
                    raise NotFoundError(
                        f"current parquet for batch {file_name!r} is no longer registered"
                    ) from exc
                current_rows, kept_rows, wrote_rewrite = _materialize(target_path)
        raise WithdrawalConflictError(f"withdrawal of {file_name!r} kept conflicting: {last_exc}")

    def _withdrawal_switch(
        self,
        day: str,
        batch_id: int,
        file_name: str,
        target_path: str,
        expected_rows: int,
        rewrite_path: Path,
        kept_rows: list,
        wrote_rewrite: bool,
    ) -> dict:
        """Single-transaction registry switch for a withdrawal."""
        now = _now_iso()
        conn = connect(str(self.db_path))
        try:
            conn.execute("BEGIN IMMEDIATE")
            target = conn.execute(
                "SELECT id, rows, state FROM files WHERE path = ?", (target_path,)
            ).fetchone()
            # The file must still be the same active registry row we computed
            # the difference against; otherwise a concurrent switch happened
            # and the caller must retry against the new file.
            if target is None or target["state"] != "active" or target["rows"] != expected_rows:
                raise WithdrawalConflictError(
                    f"target file {Path(target_path).name} changed during withdrawal"
                )
            if wrote_rewrite:
                conn.execute(
                    "INSERT INTO files(day, path, kind, state, rows, created_at)"
                    " VALUES (?, ?, 'rewrite', 'active', ?, ?)",
                    (day, str(rewrite_path), len(kept_rows), now),
                )
                conn.execute(
                    "INSERT INTO file_lineage(src_path, dst_path, op)"
                    " VALUES (?, ?, 'withdraw')",
                    (target_path, str(rewrite_path)),
                )
            else:
                # Every row of the file belonged to withdrawn batches: no
                # successor parquet is needed; lineage simply ends here.
                conn.execute(
                    "INSERT INTO file_lineage(src_path, dst_path, op)"
                    " VALUES (?, '', 'withdraw')",
                    (target_path,),
                )
            conn.execute(
                "UPDATE files SET state = 'retired' WHERE id = ?", (target["id"],)
            )
            cur = conn.execute(
                "INSERT INTO withdrawals(day, batch_id, file_name, removed_rows, created_at)"
                " VALUES (?, ?, ?, ?, ?)",
                (day, batch_id, file_name, expected_rows - len(kept_rows), now),
            )
            withdrawal_id = cur.lastrowid
            conn.execute(
                "UPDATE batches SET state = 'withdrawn', withdrawn_at = ? WHERE id = ?",
                (now, batch_id),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        log.info("withdrew batch %s: %d row(s) removed",
                 file_name, expected_rows - len(kept_rows))
        self.collect_garbage()
        with connect(str(self.db_path)) as conn:
            wd = conn.execute(
                "SELECT * FROM withdrawals WHERE id = ?", (withdrawal_id,)
            ).fetchone()
            return self._withdrawal_dict(conn, wd, already=False)

    @staticmethod
    def _withdrawal_dict(conn: sqlite3.Connection, row: sqlite3.Row, already: bool) -> dict:
        day_row_count = conn.execute(
            "SELECT COALESCE(SUM(rows), 0) FROM files WHERE day = ? AND state = 'active'",
            (row["day"],),
        ).fetchone()[0]
        file_count = conn.execute(
            "SELECT COUNT(*) FROM files WHERE day = ? AND state = 'active'",
            (row["day"],),
        ).fetchone()[0]
        return {
            "withdrawal_id": row["id"],
            "day": row["day"],
            "file": row["file_name"],
            "status": "already_withdrawn" if already else "withdrawn",
            "removed_rows": row["removed_rows"],
            "file_count": file_count,
            "row_count": day_row_count,
            "created_at": row["created_at"],
        }

    def list_withdrawals(self, day: str) -> dict:
        with connect(str(self.db_path)) as conn:
            rows = conn.execute(
                "SELECT * FROM withdrawals WHERE day = ? ORDER BY id", (day,)
            ).fetchall()
            return {
                "day": day,
                "count": len(rows),
                "withdrawals": [
                    {
                        "withdrawal_id": r["id"],
                        "file": r["file_name"],
                        "removed_rows": r["removed_rows"],
                        "created_at": r["created_at"],
                    }
                    for r in rows
                ],
            }

    def get_batch_status(self, day: str, file_name: str) -> dict:
        with connect(str(self.db_path)) as conn:
            batch = conn.execute(
                "SELECT * FROM batches WHERE day = ? AND file_name = ?",
                (day, file_name),
            ).fetchone()
            if batch is None:
                raise NotFoundError(f"no tracked upload named {file_name!r} on day {day}")
            wd = conn.execute(
                "SELECT * FROM withdrawals WHERE batch_id = ?", (batch["id"],)
            ).fetchone()
            return {
                "day": day,
                "file": file_name,
                "state": batch["state"],
                "rows": batch["rows"],
                "withdrawal": self._withdrawal_dict(conn, wd, already=True) if wd else None,
            }

    # ------------------------------------------------------------------
    # garbage collection of retired files
    # ------------------------------------------------------------------
    def collect_garbage(self) -> int:
        """Unlink retired files whose in-flight readers have all finished.

        Never touches ``active`` files, so freshly appended parquet files are
        always safe.  Retired files that belong to a still-running compaction
        snapshot are also pinned: that worker may still be reading their bytes
        when the merge started before the files were retired (e.g. a withdrawal
        that interleaved with a late compaction).
        """
        collected = 0
        with self._state_lock:
            with connect(str(self.db_path)) as conn:
                pinned: set[str] = set()
                for job in conn.execute(
                    "SELECT input_paths FROM compactions WHERE status = 'running'"
                ).fetchall():
                    pinned.update(json.loads(job["input_paths"]))
                retired = conn.execute(
                    "SELECT id, path FROM files WHERE state = 'retired'"
                ).fetchall()
                for row in retired:
                    if self._inflight.get(row["path"], 0) > 0:
                        continue  # a reader is still using these bytes
                    if row["path"] in pinned:
                        continue  # a running compaction may still be merging them
                    try:
                        os.unlink(row["path"])
                    except FileNotFoundError:
                        pass
                    except OSError:
                        log.exception("could not unlink retired file %s", row["path"])
                        continue  # keep the row, retry on the next sweep
                    conn.execute("DELETE FROM files WHERE id = ?", (row["id"],))
                    # Lineage edges are intentionally retained after the
                    # registry row of a retired file is gone: they are batch
                    # provenance metadata and a tracked batch may still need
                    # to walk through this hop long after the bytes were
                    # reclaimed.
                    collected += 1
        if collected:
            log.info("collected %d retired file(s)", collected)
        return collected

    # ------------------------------------------------------------------
    # crash recovery (run once at startup, before serving traffic)
    # ------------------------------------------------------------------
    def recover(self) -> dict:
        """Reconcile disk and registry, then resume interrupted compactions."""
        stats = {"retired_removed": 0, "orphans_removed": 0, "jobs_resumed": 0}

        with connect(str(self.db_path)) as conn:
            # 1. No reader can be in flight right after boot -> retired files
            #    are safe to remove immediately.
            retired = conn.execute("SELECT id, path FROM files WHERE state = 'retired'").fetchall()
            for row in retired:
                Path(row["path"]).unlink(missing_ok=True)
                conn.execute("DELETE FROM files WHERE id = ?", (row["id"],))
                # Lineage edges are kept: they are provenance metadata that
                # tracked batches still need after the bytes are reclaimed.
                stats["retired_removed"] += 1
            known = {r["path"] for r in conn.execute("SELECT path FROM files").fetchall()}
            running = conn.execute(
                "SELECT id, day, input_paths FROM compactions WHERE status = 'running'"
            ).fetchall()

        # 2. Sweep tmp fragments and parquet files the registry does not know
        #    (interrupted uploads, superseded compaction outputs).
        for day_dir in sorted(self.data_dir.glob("dt=*")):
            if not day_dir.is_dir():
                continue
            for f in day_dir.iterdir():
                if not f.is_file():
                    continue
                if ".tmp" in f.name or (f.suffix == ".parquet" and str(f) not in known):
                    f.unlink(missing_ok=True)
                    stats["orphans_removed"] += 1

        # 3. A job still 'running' cannot have switched the registry (switch
        #    and status update are one transaction), so its inputs are all
        #    still active: simply run it again.
        for job in running:
            input_paths = json.loads(job["input_paths"])
            log.warning("resuming interrupted compaction job %s for %s", job["id"], job["day"])
            self._launch_worker(job["id"], job["day"], input_paths)
            stats["jobs_resumed"] += 1

        log.info("recovery finished: %s", stats)
        return stats
