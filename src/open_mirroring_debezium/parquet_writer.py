"""Build Parquet files from parsed Debezium events."""

from __future__ import annotations

import io
import logging
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .debezium_parser import ParsedEvent

logger = logging.getLogger(__name__)


def _coerce_key_columns(rows: list[dict[str, Any]], key_columns: list[str]) -> None:
    """Coerce primary-key values that arrive as float (e.g. 50.0) to int in-place."""
    for row in rows:
        for col in key_columns:
            val = row.get(col)
            if isinstance(val, float) and val == int(val):
                row[col] = int(val)


def _ordered_columns(rows: list[dict[str, Any]]) -> list[str]:
    """Return a stable column order with ``__rowMarker__`` guaranteed last."""
    seen: dict[str, None] = {}
    for row in rows:
        for col in row:
            if col != "__rowMarker__":
                seen.setdefault(col, None)
    columns = list(seen)
    columns.append("__rowMarker__")
    return columns


def build_parquet(events: list[ParsedEvent], key_columns: list[str] | None = None) -> bytes:
    """Build an in-memory Parquet file from parsed events.

    ``__rowMarker__`` is always the last column.  Uses Snappy compression.
    """
    if not events:
        raise ValueError("Cannot build Parquet from an empty event list")

    rows = [e.row_data for e in events]

    if key_columns:
        _coerce_key_columns(rows, key_columns)

    columns = _ordered_columns(rows)

    # Build column arrays — let PyArrow infer types, handle nulls
    arrays: list[pa.Array] = []
    for col in columns:
        values = [row.get(col) for row in rows]
        arrays.append(pa.array(values, from_pandas=True))

    table = pa.table(dict(zip(columns, arrays, strict=False)))

    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()
