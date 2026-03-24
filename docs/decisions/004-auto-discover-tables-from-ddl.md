---
id: "004"
title: "Auto-discover tables from Debezium DDL events"
status: accepted
date: 2026-03-24
tags: [scalability, configuration]
---

## Context

The pipeline needs to handle 3 tables initially, scaling to potentially hundreds. Explicit per-table config (key columns, schema) is impractical at setup time. Debezium emits DDL/schema change events containing `primaryKeyColumnNames` and full column metadata.

## Decision

Auto-discover new tables from DDL events in the event stream. Use a two-tier config: defaults (apply to all tables) + per-table overrides in `table_config.json`. When a DDL event arrives for a new table, extract primary keys and auto-create the landing zone folder + `_metadata.json`.

## Rationale

Key columns from DDL events are the authoritative source — they come directly from the database's information schema via Debezium. Manual config would be a stale copy of the same data.

Alternatives considered:

- **Explicit per-table config** — doesn't scale. Every new table requires a config change and redeployment.
- **Query source database directly** — adds a dependency back to the source, which defeats the purpose of event-driven architecture.

## Consequences

- First DDL event for a table must arrive before data events for key columns to be set correctly.
- If data arrives first, the table is created without key columns (inserts only, no updates/deletes). Logging warns when this happens.
- Once `keyColumns` is set in `_metadata.json`, it cannot be changed (Open Mirroring constraint) — so the first DDL event wins.
- Overrides in `table_config.json` still available for edge cases (e.g., tables with no primary key that need a logical key).
