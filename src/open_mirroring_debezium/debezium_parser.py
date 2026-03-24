"""Parse Debezium CDC events from EventHub."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

# Debezium op → Open Mirroring __rowMarker__
# 0 = insert/snapshot, 4 = update, 2 = delete
OP_TO_MARKER: dict[str, int] = {"c": 0, "r": 0, "u": 4, "d": 2}


@dataclass
class ParsedEvent:
    """A parsed Debezium data-change event."""

    schema_name: str
    table_name: str
    op: str
    row_marker: int
    row_data: dict[str, Any]
    ts_ms: int


@dataclass
class ParsedDDL:
    """A parsed Debezium schema-change (DDL) event."""

    schema_name: str
    table_name: str
    ddl_text: str
    primary_key_columns: list[str] = field(default_factory=list)
    columns: list[dict[str, Any]] = field(default_factory=list)


def _extract_ddl(body: dict[str, Any]) -> ParsedDDL | None:
    """Extract DDL information from a Debezium SchemaChangeValue message."""
    payload = body.get("payload", {})
    ddl_text = payload.get("ddl", "")
    source = payload.get("source", {})
    schema_name = source.get("schema", "")
    table_name = source.get("table", "")

    primary_keys: list[str] = []
    columns: list[dict[str, Any]] = []

    for change in payload.get("tableChanges", []):
        tbl = change.get("table", {})
        # Primary key column names
        primary_keys = tbl.get("primaryKeyColumnNames", [])
        # Column metadata
        columns = tbl.get("columns", [])
        # If the table identity is in the change, prefer it
        table_id = change.get("id", "")
        if table_id and not table_name:
            # id is typically "SCHEMA.TABLE" or similar
            parts = table_id.replace('"', "").split(".")
            if len(parts) >= 2:
                schema_name = schema_name or parts[-2]
                table_name = table_name or parts[-1]

    if not table_name:
        logger.debug("DDL event with no identifiable table — skipping: %s", ddl_text[:120])
        return None

    return ParsedDDL(
        schema_name=schema_name,
        table_name=table_name,
        ddl_text=ddl_text,
        primary_key_columns=primary_keys,
        columns=columns,
    )


def _is_schema_change(body: dict[str, Any]) -> bool:
    """Return True if the message schema indicates a DDL / schema-change event."""
    schema = body.get("schema", {})
    schema_name = schema.get("name", "")
    return "SchemaChangeValue" in schema_name


def parse_event(body: dict[str, Any]) -> ParsedEvent | ParsedDDL | None:
    """Parse a single Debezium message body.

    Returns:
        ParsedEvent for data-change events (c/r/u/d),
        ParsedDDL for schema-change events,
        None for unrecognised messages.
    """
    # DDL / schema-change events
    if _is_schema_change(body):
        return _extract_ddl(body)

    payload = body.get("payload")
    if payload is None:
        logger.warning("Message has no payload — skipping")
        return None

    op = payload.get("op")
    if op is None:
        # No op and not a schema change — skip
        return None

    if op not in OP_TO_MARKER:
        logger.warning("Unknown Debezium op '%s' — skipping", op)
        return None

    source = payload.get("source")
    if source is None:
        logger.warning("Payload missing 'source' field (op=%s) — skipping", op)
        return None

    schema_name: str = source.get("schema", "")
    table_name: str = source.get("table", "")
    ts_ms: int = payload.get("ts_ms", 0)

    # Row data: deletes use "before", everything else uses "after"
    if op == "d":
        before = payload.get("before")
        if before is None:
            logger.warning("Delete event missing 'before' data (table=%s.%s) — skipping", schema_name, table_name)
            return None
        row_data: dict[str, Any] = dict(before)
    else:
        after = payload.get("after")
        if after is None:
            logger.warning(
                "Non-delete event (op=%s) missing 'after' data (table=%s.%s) — skipping",
                op,
                schema_name,
                table_name,
            )
            return None
        row_data = dict(after)

    row_marker: int = OP_TO_MARKER[op]
    row_data["__rowMarker__"] = row_marker

    return ParsedEvent(
        schema_name=schema_name,
        table_name=table_name,
        op=op,
        row_marker=row_marker,
        row_data=row_data,
        ts_ms=ts_ms,
    )
