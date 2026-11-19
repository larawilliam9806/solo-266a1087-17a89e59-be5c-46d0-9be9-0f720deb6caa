"""End-to-end tests for the batch withdrawal feature."""

from __future__ import annotations

import sys
import threading
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import app.lake as lake_mod  # noqa: E402
from app.csvio import empty_table  # noqa: E402
from app.main import create_app  # noqa: E402
from app.storage import write_parquet_atomic  # noqa: E402

DAY = "2026-11-18"
ANOTHER_DAY = "2026-11-19"

CSV_A = (
    "buoy_id,ts_utc,salinity\n"
    "B001,2026-11-18T00:01:00Z,35.1\n"
    "B002,2026-11-18T00:02:00Z,35.2\n"
)
CSV_B = (
    "buoy_id,ts_utc,salinity\n"
    # same value as a row in A: must survive A's withdrawal (B still contributes it)
    "B001,2026-11-18T00:01:00Z,35.1\n"
    "B003,2026-11-18T01:02:00Z,34.9\n"
)
CSV_C = "buoy_id,ts_utc,salinity\nB004,2026-11-18T02:00:00Z,36.0\n"
# every row duplicated inside one batch: all occurrences must go together
CSV_DUP = (
    "buoy_id,ts_utc,salinity\n"
    "B009,2026-11-18T03:00:00Z,33.3\n"
    "B009,2026-11-18T03:00:00Z,33.3\n"
)


def upload(client: TestClient, csv_text: str, day: str = DAY) -> dict:
    r = client.post(
        f"/days/{day}/upload",
        files={"file": ("obs.csv", csv_text.encode(), "text/csv")},
    )
    assert r.status_code == 200, r.text
    return r.json()


def withdraw(client: TestClient, file_name: str, day: str = DAY):
    return client.post(f"/days/{day}/withdrawals", json={"file": file_name})


def wait_job(client: TestClient, day: str, job_id: int, timeout: float = 15.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = client.get(f"/days/{day}/compactions/{job_id}").json()
        if job["status"] in ("done", "failed"):
            return job
        time.sleep(0.05)
    pytest.fail(f"compaction job {job_id} did not finish in {timeout}s")


def keys(body: dict) -> set:
    return {
        (rec["buoy_id"], rec["ts_utc"], rec["salinity"])
        for rec in body["records"]
    }


def make_client(tmp_path, **kwargs) -> TestClient:
    return TestClient(create_app(data_dir=str(tmp_path / "data"), **kwargs))


# ---------------------------------------------------------------------------
# basic withdrawal semantics
# ---------------------------------------------------------------------------

def test_upload_reports_withdrawable_flag(tmp_path):
    with make_client(tmp_path) as c:
        body = upload(c, CSV_A)
        assert body["withdrawable"] is True
        assert c.get("/").json()["withdrawals_enabled"] is True


def test_withdraw_removes_batch_rows_but_keeps_same_values_from_other_batches(tmp_path):
    with make_client(tmp_path) as c:
        up_a = upload(c, CSV_A)
        up_b = upload(c, CSV_B)
        before = c.get(f"/days/{DAY}/records").json()
        assert before["row_count"] == 4

        r = withdraw(c, up_a["file"])
        assert r.status_code == 200, r.text
        wd = r.json()
        assert wd["status"] == "withdrawn"
        assert wd["removed_rows"] == 2
        assert wd["row_count"] == 2 and wd["file_count"] == 1

        after = c.get(f"/days/{DAY}/records").json()
        # B001/35.1 survives because batch B still contributes that value;
        # B002 only existed in A and is gone.
        assert keys(after) == {
            ("B001", "2026-11-18T00:01:00Z", 35.1),
            ("B003", "2026-11-18T01:02:00Z", 34.9),
        }
        assert after["row_count"] == 2 and after["file_count"] == 1
        # records still expose exactly the original three columns
        assert set(after["records"][0]) == {"buoy_id", "ts_utc", "salinity"}

        # batch B is untouched
        assert c.get(f"/days/{DAY}/uploads/{up_b['file']}").json()["state"] == "active"


def test_withdraw_removes_internal_duplicates_as_a_multiset(tmp_path):
    with make_client(tmp_path) as c:
        up_dup = upload(c, CSV_DUP)
        upload(c, CSV_C)
        assert c.get(f"/days/{DAY}/records").json()["row_count"] == 3

        r = withdraw(c, up_dup["file"])
        assert r.status_code == 200
        assert r.json()["removed_rows"] == 2
        body = c.get(f"/days/{DAY}/records").json()
        assert body["row_count"] == 1
        assert body["records"][0]["buoy_id"] == "B004"


def test_withdrawing_every_batch_leaves_empty_day_but_three_column_schema(tmp_path):
    with make_client(tmp_path) as c:
        up_a = upload(c, CSV_A)
        up_b = upload(c, CSV_B)
        assert withdraw(c, up_a["file"]).status_code == 200
        r = withdraw(c, up_b["file"])
        assert r.status_code == 200
        assert r.json()["row_count"] == 0 and r.json()["file_count"] == 0
        body = c.get(f"/days/{DAY}/records").json()
        assert body["row_count"] == 0 and body["file_count"] == 0 and body["records"] == []
        assert c.get(f"/days/{DAY}/files").json()["file_count"] == 0


def test_repeated_withdrawal_is_idempotent(tmp_path):
    with make_client(tmp_path) as c:
        up_a = upload(c, CSV_A)
        first = withdraw(c, up_a["file"]).json()
        second = withdraw(c, up_a["file"])
        assert second.status_code == 200
        body = second.json()
        assert body["status"] == "already_withdrawn"
        assert body["withdrawal_id"] == first["withdrawal_id"]
        assert body["removed_rows"] == first["removed_rows"]
        # still exactly one withdrawal record
        listed = c.get(f"/days/{DAY}/withdrawals").json()
        assert listed["count"] == 1
        assert listed["withdrawals"][0]["file"] == up_a["file"]


def test_unknown_file_name_returns_404(tmp_path):
    with make_client(tmp_path) as c:
        upload(c, CSV_A)
        r = withdraw(c, "part-doesnotexist.parquet")
        assert r.status_code == 404
        assert c.get(f"/days/{DAY}/uploads/part-doesnotexist.parquet").status_code == 404
        # no data changed
        assert c.get(f"/days/{DAY}/records").json()["row_count"] == 2


# ---------------------------------------------------------------------------
# feature gating: only batches uploaded while enabled are withdrawable
# ---------------------------------------------------------------------------

def test_uploads_while_disabled_are_not_withdrawable(tmp_path):
    with make_client(tmp_path, withdrawals_enabled=False) as c:
        up = upload(c, CSV_A)
        assert up["withdrawable"] is False
        r = withdraw(c, up["file"])
        assert r.status_code == 409
        assert "before withdrawals were enabled" in r.json()["detail"]
        assert c.get(f"/days/{DAY}/records").json()["row_count"] == 2

    # re-enabling later does not retroactively track the old upload
    with make_client(tmp_path, withdrawals_enabled=True) as c2:
        r = withdraw(c, up["file"])
        assert r.status_code == 409
        new_up = upload(c2, CSV_C)
        assert new_up["withdrawable"] is True
        assert withdraw(c2, new_up["file"]).status_code == 200
        # legacy rows remain; only the tracked batch's row is gone
        body = c2.get(f"/days/{DAY}/records").json()
        assert body["row_count"] == 2


def test_legacy_rows_survive_withdrawal_after_restart(tmp_path):
    """Rows that cannot be attributed to any tracked batch are always kept."""
    data_dir = tmp_path / "data"
    with make_client(tmp_path, withdrawals_enabled=False) as c:
        upload(c, CSV_A)  # untracked, unattributable after this point
    with make_client(tmp_path, withdrawals_enabled=True) as c2:
        up_b = upload(c2, CSV_B)
        withdraw(c2, up_b["file"])
        body = c2.get(f"/days/{DAY}/records").json()
        assert keys(body) == {
            ("B001", "2026-11-18T00:01:00Z", 35.1),
            ("B002", "2026-11-18T00:02:00Z", 35.2),
        }


# ---------------------------------------------------------------------------
# withdrawal interleaved with compaction and garbage collection
# ---------------------------------------------------------------------------

def test_withdraw_by_original_name_after_compaction_and_gc(tmp_path):
    with make_client(tmp_path, gc_interval_sec=0.1) as c:
        up_a = upload(c, CSV_A)
        up_b = upload(c, CSV_B)
        job = wait_job(c, DAY, c.post(f"/days/{DAY}/compactions").json()["job_id"])
        assert job["status"] == "done"

        # wait until the original source bytes have been reclaimed
        day_dir = tmp_path / "data" / f"dt={DAY}"
        deadline = time.time() + 10
        while time.time() < deadline:
            names = {p.name for p in day_dir.glob("*.parquet")}
            if up_a["file"] not in names and up_b["file"] not in names:
                break
            time.sleep(0.1)
        else:
            pytest.fail("source files were not garbage collected")

        # the original upload file name is still a valid withdrawal handle
        r = withdraw(c, up_a["file"])
        assert r.status_code == 200, r.text
        body = c.get(f"/days/{DAY}/records").json()
        assert keys(body) == {
            ("B001", "2026-11-18T00:01:00Z", 35.1),
            ("B003", "2026-11-18T01:02:00Z", 34.9),
        }


def test_withdraw_then_compact_then_withdraw_again(tmp_path):
    with make_client(tmp_path, gc_interval_sec=0.1) as c:
        up_a = upload(c, CSV_A)
        up_b = upload(c, CSV_B)
        up_c = upload(c, CSV_C)
        assert withdraw(c, up_a["file"]).status_code == 200
        job = wait_job(c, DAY, c.post(f"/days/{DAY}/compactions").json()["job_id"])
        assert job["status"] == "done"
        # B and C are now together in one compacted file; withdrawing B must
        # follow provenance across the compaction and the earlier rewrite.
        r = withdraw(c, up_b["file"])
        assert r.status_code == 200, r.text
        body = c.get(f"/days/{DAY}/records").json()
        assert body["row_count"] == 1 and body["records"][0]["buoy_id"] == "B004"
        assert withdraw(c, up_c["file"]).json()["row_count"] == 0


def test_late_compaction_after_withdrawal_fails_and_never_resurrects_rows(tmp_path):
    with make_client(tmp_path, compaction_delay_sec=0.8, gc_interval_sec=0.05) as c:
        up_a = upload(c, CSV_A)
        upload(c, CSV_B)
        job_id = c.post(f"/days/{DAY}/compactions").json()["job_id"]
        time.sleep(0.2)  # let the worker snapshot inputs and start merging

        wd = withdraw(c, up_a["file"])
        assert wd.status_code == 200
        body_mid = c.get(f"/days/{DAY}/records").json()
        assert body_mid["row_count"] == 2  # only batch B remains

        job = wait_job(c, DAY, job_id)
        # the late compaction must be rejected at the atomic switch guard
        assert job["status"] == "failed"
        assert "withdrawn" in job["error"]

        body = c.get(f"/days/{DAY}/records").json()
        assert body["row_count"] == 2
        assert keys(body) == {
            ("B001", "2026-11-18T00:01:00Z", 35.1),
            ("B003", "2026-11-18T01:02:00Z", 34.9),
        }
        # a follow-up compaction over the post-withdrawal state works
        follow = wait_job(c, DAY, c.post(f"/days/{DAY}/compactions").json()["job_id"])
        assert follow["status"] == "done"
        assert keys(c.get(f"/days/{DAY}/records").json()) == keys(body)


def test_reads_during_withdrawal_see_a_single_consistent_state(tmp_path):
    """Every read is either fully pre- or fully post-withdrawal."""
    with make_client(tmp_path) as c:
        up_a = upload(c, CSV_A)
        upload(c, CSV_B)
        seen = []
        errors = []
        stop = threading.Event()

        def reader():
            try:
                while not stop.is_set():
                    body = c.get(f"/days/{DAY}/records").json()
                    seen.append((body["file_count"], body["row_count"]))
                    time.sleep(0.005)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=reader) for _ in range(4)]
        for t in threads:
            t.start()
        r = withdraw(c, up_a["file"])
        assert r.status_code == 200
        time.sleep(0.2)
        stop.set()
        for t in threads:
            t.join()
        assert not errors, errors
        # 2 files / 4 rows before the switch, 1 file / 2 rows after --
        # never a torn intermediate state.
        assert set(seen) <= {(2, 4), (1, 2)}
        assert (2, 4) in seen and (1, 2) in seen


def test_retired_file_waiting_on_inflight_reader_is_not_unlinked(tmp_path):
    with make_client(tmp_path, gc_interval_sec=3600) as c:
        up_a = upload(c, CSV_A)
        upload(c, CSV_B)
        lake = c.app.state.lake
        # pin the upload file exactly like an in-flight read would
        paths = lake._snapshot_active(DAY)
        assert any(p.endswith(up_a["file"]) for p in paths)
        retired_path = next(p for p in paths if p.endswith(up_a["file"]))
        try:
            r = withdraw(c, up_a["file"])
            assert r.status_code == 200
            # GC must not reclaim the retired bytes while this read is active
            assert lake.collect_garbage() == 0
            assert Path(retired_path).exists()
        finally:
            lake._release(paths)  # releases and sweeps once refs hit zero
        # the retired bytes are gone after the reader drained, but the data
        # exposed by the API is of course still complete
        assert not Path(retired_path).exists()
        body = c.get(f"/days/{DAY}/records").json()
        assert body["row_count"] == 2


# ---------------------------------------------------------------------------
# durability and failure handling
# ---------------------------------------------------------------------------

def test_successful_withdrawal_survives_restart(tmp_path):
    data_dir = tmp_path / "data"
    with make_client(tmp_path) as c:
        up_a = upload(c, CSV_A)
        upload(c, CSV_B)
        assert withdraw(c, up_a["file"]).status_code == 200

    with make_client(tmp_path) as c2:
        body = c2.get(f"/days/{DAY}/records").json()
        assert keys(body) == {
            ("B001", "2026-11-18T00:01:00Z", 35.1),
            ("B003", "2026-11-18T01:02:00Z", 34.9),
        }
        # no resurrection; repeat call reports the durable withdrawal
        r = withdraw(c2, up_a["file"])
        assert r.json()["status"] == "already_withdrawn"
        assert c2.get(f"/days/{DAY}/uploads/{up_a['file']}").json()["state"] == "withdrawn"


def test_failed_rewrite_write_keeps_original_data_and_active_state(tmp_path, monkeypatch):
    with make_client(tmp_path) as c:
        up_a = upload(c, CSV_A)
        upload(c, CSV_B)
        # compact first (real writes) so a withdrawal rewrite would be non-empty
        job = wait_job(c, DAY, c.post(f"/days/{DAY}/compactions").json()["job_id"])
        assert job["status"] == "done"
        assert c.get(f"/days/{DAY}/records").json()["file_count"] == 1

        def boom(*args, **kwargs):
            raise OSError("disk full (simulated)")

        # lake.py imports the function by name, so patch it where it is looked up
        monkeypatch.setattr("app.lake.write_parquet_atomic", boom)
        r = withdraw(c, up_a["file"])
        assert r.status_code == 500
        assert "disk full" in r.json()["detail"]

        monkeypatch.undo()
        body = c.get(f"/days/{DAY}/records").json()
        # compaction preserves every occurrence (4 rows); the failed
        # withdrawal changed neither the registry nor the bytes.
        assert body["row_count"] == 4 and body["file_count"] == 1
        assert c.get(f"/days/{DAY}/uploads/{up_a['file']}").json()["state"] == "active"
        # retry after the "disk" recovers
        assert withdraw(c, up_a["file"]).status_code == 200
        body = c.get(f"/days/{DAY}/records").json()
        assert body["row_count"] == 2
        assert keys(body) == {
            ("B001", "2026-11-18T00:01:00Z", 35.1),
            ("B003", "2026-11-18T01:02:00Z", 34.9),
        }


class _SwitchFailingLake(lake_mod.Lake):
    fail_next_switch = True

    def _withdrawal_switch(self, *args, **kwargs):
        if self.fail_next_switch:
            self.fail_next_switch = False
            raise RuntimeError("metadata db unavailable (simulated)")
        return super()._withdrawal_switch(*args, **kwargs)


def test_registry_switch_failure_keeps_original_data(tmp_path):
    app = create_app(data_dir=str(tmp_path / "data"))
    with TestClient(app, raise_server_exceptions=False) as c:
        flaky = _SwitchFailingLake(str(tmp_path / "data"))
        c.app.state.lake = flaky
        up_a = upload(c, CSV_A)
        upload(c, CSV_B)

        r = withdraw(c, up_a["file"])
        assert r.status_code == 500
        body = c.get(f"/days/{DAY}/records").json()
        assert body["row_count"] == 4 and body["file_count"] == 2
        assert c.get(f"/days/{DAY}/uploads/{up_a['file']}").json()["state"] == "active"
        # retry succeeds; nothing was half-committed
        assert withdraw(c, up_a["file"]).status_code == 200
        assert c.get(f"/days/{DAY}/records").json()["row_count"] == 2


def test_unregistered_rewrite_bytes_are_swept_on_restart(tmp_path):
    data_dir = tmp_path / "data"
    lake = lake_mod.Lake(str(data_dir))
    lake.upload(DAY, CSV_A.encode())
    day_dir = data_dir / f"dt={DAY}"
    write_parquet_atomic(empty_table(), day_dir / "rewrite-orphan.parquet")
    (day_dir / "rewrite-x.parquet.tmp-deadbeef").write_bytes(b"partial")
    del lake

    with make_client(tmp_path) as c:
        assert c.get(f"/days/{DAY}/records").json()["row_count"] == 2
        names = {p.name for p in day_dir.glob("*")}
        assert "rewrite-orphan.parquet" not in names
        assert not any(".tmp" in n for n in names)


def test_withdrawal_does_not_leak_between_days(tmp_path):
    with make_client(tmp_path) as c:
        up_a = upload(c, CSV_A, day=DAY)
        upload(c, CSV_C, day=ANOTHER_DAY)
        # same file name cannot withdraw from a different day
        assert withdraw(c, up_a["file"], day=ANOTHER_DAY).status_code == 404
        assert withdraw(c, up_a["file"], day=DAY).status_code == 200
        assert c.get(f"/days/{ANOTHER_DAY}/records").json()["row_count"] == 1
        assert c.get(f"/days/{DAY}/records").json()["row_count"] == 0
