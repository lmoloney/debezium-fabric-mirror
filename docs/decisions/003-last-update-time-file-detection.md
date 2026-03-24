---
id: "003"
title: "LastUpdateTimeFileDetection over sequential numbering"
status: accepted
date: 2026-03-24
tags: [open-mirroring, reliability]
---

## Context

Open Mirroring supports two file detection strategies:

1. **Sequential file numbering** (default) — files must be named `00000000000000000001.parquet`, `00000000000000000002.parquet`, etc.
2. **LastUpdateTimeFileDetection** — files read by modification timestamp, no naming convention required.

## Decision

Use `fileDetectionStrategy: LastUpdateTimeFileDetection` in `_metadata.json`. Combine with `isUpsertDefaultRowMarker: true` for rows without explicit markers.

## Rationale

Sequential numbering requires list-then-increment which isn't atomic — race conditions under concurrency. With hundreds of tables and batch processing, timestamp-based detection is simpler and safer.

Alternatives considered:

- **Sequential numbering** — deterministic ordering, but requires distributed coordination to avoid naming collisions. Not worth the complexity.

## Consequences

- File ordering is implicit (by timestamp) rather than explicit (by name). This is fine for CDC where row-level ordering within files preserves transaction order.
- Gives flexibility for file naming (can use UUIDs, timestamps, etc.).
- No coordination needed between concurrent writers to the same table folder.
