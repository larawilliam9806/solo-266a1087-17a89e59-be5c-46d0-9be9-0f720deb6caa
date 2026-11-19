"""Row-multiset helpers used by the withdrawal feature.

Rows are identified by the full canonical tuple
``(buoy_id, ts_utc, salinity)``.  A batch's rows are stored durably in SQLite
with the timestamp rendered as integer microseconds since the UTC epoch, which
round-trips exactly through the canonical parquet schema
(``timestamp('us', tz='UTC')``) and the JSON read response.

Withdrawal semantics are *multiset difference*: every row contributed by the
withdrawn batch is removed (including duplicates inside the batch), while the
same values contributed by other batches, and pre-existing rows that cannot be
attributed to any tracked batch, are retained.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq

ROW = tuple[str, int, float]
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_US = timedelta(microseconds=1)


def dt_to_us(dt: datetime) -> int:
    """Naive datetimes are treated as UTC, mirroring CSV parsing."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int((dt.astimezone(timezone.utc) - _EPOCH) // _US)


def us_to_dt(ts_us: int) -> datetime:
    return datetime.fromtimestamp(ts_us / 1_000_000, tz=timezone.utc)


def table_to_rows(table: pa.Table) -> list[ROW]:
    """Extract canonical row tuples, preserving parquet row order."""
    cols = table.to_pydict()
    return [
        (b, dt_to_us(t), float(s))
        for b, t, s in zip(cols["buoy_id"], cols["ts_utc"], cols["salinity"])
    ]


def rows_to_table(rows: list[ROW]) -> pa.Table:
    """Rebuild a canonical-schema parquet table from row tuples."""
    from .csvio import CANONICAL_SCHEMA

    return pa.Table.from_pydict(
        {
            "buoy_id": [r[0] for r in rows],
            "ts_utc": pa.array([us_to_dt(r[1]) for r in rows],
                               type=pa.timestamp("us", tz="UTC")),
            "salinity": [r[2] for r in rows],
        },
        schema=CANONICAL_SCHEMA,
    )


def read_parquet_rows(path: str) -> list[ROW]:
    return table_to_rows(pq.read_table(path))


def multiset_difference(rows: list[ROW], subtract: list[ROW]) -> list[ROW]:
    """Return ``rows`` minus ``subtract`` as multisets, keeping original order.

    Each occurrence in ``subtract`` cancels at most one equal occurrence in
    ``rows``; values that other batches still contribute (or unattributed
    legacy rows) survive.  ``subtract`` being a strict sub-multiset of the
    target file's rows is an invariant enforced by the lineage design.
    """
    remaining = Counter(subtract)
    out: list[ROW] = []
    for row in rows:
        if remaining.get(row, 0) > 0:
            remaining[row] -= 1
        else:
            out.append(row)
    return out
