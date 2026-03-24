---
id: "002"
title: "Upsert over Update for Debezium change events"
status: accepted
date: 2026-03-24
tags: [data, open-mirroring]
---

## Context

Debezium emits `op: "u"` for updates. Open Mirroring supports both Update (`__rowMarker__=1`) and Upsert (`__rowMarker__=4`). Need to choose which to map Debezium updates to.

## Decision

Map Debezium `op: "u"` to Upsert (`__rowMarker__=4`), not Update (`__rowMarker__=1`). Enable `isUpsertDefaultRowMarker: true` in metadata as a safety net.

## Rationale

If a row doesn't exist in the target (e.g., incomplete initial snapshot), Update (1) would still insert silently per the docs. Upsert makes the intent explicit and is the documented recommended default. The behavior is identical when the row already exists.

Alternatives considered:

- **Update (1)** — stricter semantics in theory, but the docs say it inserts anyway if the row is missing, so the strictness is illusory.

## Consequences

- Slightly less strict semantics — we won't catch cases where an update arrives for a genuinely non-existent row (which would be a bug in the source).
- Acceptable trade-off for robustness against incomplete snapshots.
- `isUpsertDefaultRowMarker: true` means rows without an explicit marker default to upsert, reducing the risk of silent data loss.
