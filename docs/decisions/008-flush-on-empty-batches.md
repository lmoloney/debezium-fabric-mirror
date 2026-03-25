---
id: "008"
title: "Flush buffered events on empty EventHub batches"
status: accepted
date: 2026-03-25
tags: [bugfix, buffering, reliability]
---

## Context

The EventHub SDK calls `process_batch()` on a `max_wait_time` cadence even when no
new events have arrived (delivering an empty batch). The consumer's early-return path
for empty batches skipped the `tables_ready()` check, meaning time-based flushes
(`FLUSH_MAX_INTERVAL_SECONDS`) never triggered unless new events arrived.

In practice, tables with fewer than `FLUSH_MIN_RECORDS` events would sit in the buffer
indefinitely until either more events arrived or the consumer was shut down (which
triggers `flush_all()`).

## Decision

Check `tables_ready()` on the empty-batch code path and flush any tables that have
exceeded `max_interval_seconds`. Checkpoint is updated after a successful time-based
flush, same as event-driven flushes.

Also raised the default `FLUSH_MAX_INTERVAL_SECONDS` from 30s to 60s — the original
30s was chosen before the bug was discovered and effectively never fired. 60s is a
better default for the latency/throughput trade-off now that it actually works.

## Consequences

- **Positive**: Tables with low event volume now flush reliably within `max_interval_seconds`.
- **Positive**: No buffered data is silently held until shutdown.
- **Neutral**: Slightly more checkpoint updates during quiet periods (one per flush cycle
  per partition with buffered data).
