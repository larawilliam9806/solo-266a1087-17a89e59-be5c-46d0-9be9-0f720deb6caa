"""SQLite metadata layer.

Tables
------
* ``files``        -- registry of every parquet file that belongs to a daily
                      partition.  ``state`` is ``active`` (visible to readers)
                      or ``retired`` (replaced by compaction or a withdrawal
                      rewrite, waiting for in-flight readers to drain before
                      the bytes are unlinked).
* ``compactions``  -- one row per compaction job.  A partial unique index
                      guarantees at most one ``running`` job per day, which is
                      what lets us reject concurrent compactions for the same
                      day atomically (even across a process restart).
* ``batches``      -- one row per upload made while the withdrawal feature is
                      enabled.  The original upload file name is the stable
                      withdrawal handle even after the underlying bytes have
                      been compacted away and reclaimed.
* ``batch_rows``   -- durable multiset of the rows of every tracked batch, so
                      a batch stays withdrawable after its source parquet was
                      merged into a compaction output and garbage collected.
* ``withdrawals``  -- one row per successfully withdrawn batch; makes the
                      outcome durable (no resurrection after restart) and
                      repeat withdrawal calls idempotent.
* ``file_lineage`` -- directed edges between parquet files produced by
                      compaction (many inputs -> one output) and by withdrawal
                      rewrites (one input -> one output).  Walking the edges
                      locates the currently active file carrying a batch's
                      rows.  Edges whose source registry row has been garbage
                      collected are removed together with it.

The compaction *switch* (retire inputs + register output + mark job done) and
the withdrawal *switch* (retire target + register rewrite + mark batch
withdrawn) each happen in a single SQLite transaction, so a crash can never
leave the registry half-switched.
"""

from __future__ import annotations

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    day        TEXT NOT NULL,
    path       TEXT NOT NULL UNIQUE,
    kind       TEXT NOT NULL CHECK (kind IN ('upload', 'compacted', 'rewrite')),
    state      TEXT NOT NULL CHECK (state IN ('active', 'retired')),
    rows       INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_files_day_state ON files(day, state);

CREATE TABLE IF NOT EXISTS compactions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    day         TEXT NOT NULL,
    status      TEXT NOT NULL CHECK (status IN ('running', 'done', 'failed')),
    input_paths TEXT NOT NULL,          -- JSON array, snapshot taken at job start
    input_rows  INTEGER NOT NULL,
    output_path TEXT,
    output_rows INTEGER,
    error       TEXT,
    created_at  TEXT NOT NULL,
    finished_at TEXT
);
-- At most one running compaction per day (enforced by the DB itself).
CREATE UNIQUE INDEX IF NOT EXISTS ux_compactions_one_running_per_day
    ON compactions(day) WHERE status = 'running';

-- Upload batches tracked for withdrawal (only uploads made while the
-- withdrawal feature is enabled get a row here).
CREATE TABLE IF NOT EXISTS batches (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    day          TEXT NOT NULL,
    file_name    TEXT NOT NULL UNIQUE,   -- handle returned by the upload response
    file_path    TEXT NOT NULL,          -- parquet path originally registered
    rows         INTEGER NOT NULL,
    state        TEXT NOT NULL CHECK (state IN ('active', 'withdrawn')),
    created_at   TEXT NOT NULL,
    withdrawn_at TEXT
);
CREATE INDEX IF NOT EXISTS ix_batches_day ON batches(day);

-- Durable row multiset of a tracked batch (microsecond UTC timestamps so the
-- values round-trip exactly through parquet).
CREATE TABLE IF NOT EXISTS batch_rows (
    batch_id INTEGER NOT NULL REFERENCES batches(id),
    buoy_id  TEXT NOT NULL,
    ts_us    INTEGER NOT NULL,
    salinity DOUBLE NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_batch_rows_batch ON batch_rows(batch_id);

CREATE TABLE IF NOT EXISTS withdrawals (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    day          TEXT NOT NULL,
    batch_id     INTEGER NOT NULL UNIQUE REFERENCES batches(id),
    file_name    TEXT NOT NULL,
    removed_rows INTEGER NOT NULL,
    created_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_withdrawals_day ON withdrawals(day);

-- Parquet provenance edges: src parquet was replaced by dst parquet through
-- 'compaction' or a withdrawal 'rewrite'.
CREATE TABLE IF NOT EXISTS file_lineage (
    src_path TEXT NOT NULL,
    dst_path TEXT NOT NULL,
    op       TEXT NOT NULL CHECK (op IN ('compaction', 'withdraw'))
);
CREATE INDEX IF NOT EXISTS ix_lineage_src ON file_lineage(src_path);
CREATE INDEX IF NOT EXISTS ix_lineage_dst ON file_lineage(dst_path);
"""


def connect(db_path: str) -> sqlite3.Connection:
    """Open a short-lived connection with sane durability/concurrency pragmas."""
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    # FULL so a committed upload / switch survives a power cut.
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _migrate_legacy_files_table(conn: sqlite3.Connection) -> None:
    """Databases created before withdrawals existed restrict files.kind to
    ('upload','compacted').  SQLite cannot alter a CHECK constraint in place,
    so rebuild the table when the old definition is detected, preserving every
    registered row."""
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='files'"
    ).fetchone()
    if row is not None and "'rewrite'" not in (row["sql"] or ""):
        conn.executescript(
            """
            ALTER TABLE files RENAME TO files_legacy;
            CREATE TABLE files (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                day        TEXT NOT NULL,
                path       TEXT NOT NULL UNIQUE,
                kind       TEXT NOT NULL CHECK (kind IN ('upload', 'compacted', 'rewrite')),
                state      TEXT NOT NULL CHECK (state IN ('active', 'retired')),
                rows       INTEGER NOT NULL,
                created_at TEXT NOT NULL
            );
            INSERT INTO files(id, day, path, kind, state, rows, created_at)
                SELECT id, day, path, kind, state, rows, created_at FROM files_legacy;
            DROP TABLE files_legacy;
            CREATE INDEX IF NOT EXISTS ix_files_day_state ON files(day, state);
            """
        )


def init_schema(db_path: str) -> None:
    conn = connect(db_path)
    try:
        _migrate_legacy_files_table(conn)
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()
