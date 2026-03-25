---
id: "007"
title: "Per-table event buffering with configurable flush thresholds"
status: accepted
date: 2026-03-24
tags: [performance, buffering, configuration]
---

## Context

The consumer previously wrote a Parquet file for every EventHub batch received. With
small batch sizes (as few as 1 event), this produced many tiny Parquet files — inefficient
for OneLake mirroring and wasteful of API calls.

## Decision

Add an application-level `EventBuffer` that accumulates events per table and flushes based
on four configurable thresholds:

| Env var | Default | Purpose |
|---------|---------|---------|
| `FLUSH_MIN_RECORDS` | 100 | Don't flush until at least this many records per table |
| `FLUSH_MAX_RECORDS` | 10,000 | Flush immediately at this count (memory bound) |
| `FLUSH_MIN_INTERVAL_SECONDS` | 1.0 | Rate-limit uploads during burst traffic |
| `FLUSH_MAX_INTERVAL_SECONDS` | 60.0 | Always flush after this long (latency bound) |

Checkpoint only advances after data is successfully flushed to OneLake. Buffered-but-not-
flushed events are intentionally not checkpointed — EventHub re-delivers them on restart,
and upsert semantics make this safe.

## Consequences

- **Positive**: Larger Parquet files, fewer API calls, configurable latency/throughput trade-off.
- **Acceptable risk**: On crash, buffered events are lost but will be re-delivered by EventHub.
  Maximum data in flight is bounded by `FLUSH_MAX_RECORDS × number_of_tables`.
