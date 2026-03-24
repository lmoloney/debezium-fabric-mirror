---
id: "001"
title: "Container Apps over Azure Functions"
status: accepted
date: 2026-03-24
tags: [compute, architecture]
---

## Context

Need a compute host to consume Debezium CDC events from Azure EventHub and write Parquet to OneLake. Azure Functions Python v2 (isolated worker) does NOT support `cardinality="many"` for EventHub triggers as of March 2026 — each invocation receives a single event. This makes batching impossible in Functions.

## Decision

Use Azure Container Apps with the `azure-eventhub` Python SDK's `receive_batch()` for full batch control. Scale with KEDA on EventHub consumer group lag.

## Rationale

Alternatives considered:

- **(A) Functions Python v2 single-event** — chatty, 1 Parquet per event, expensive at scale.
- **(B) Functions Python v1** — supports batching but deprecated path.
- **(C) C# Functions** — native batch support but not Python.

None of these give us Python + batching on a supported runtime. Container Apps with the SDK gives full control over batch size, flush intervals, and backpressure.

## Consequences

- More operational complexity (Dockerfile, KEDA config).
- Proper batching → lower cost at scale (hundreds of tables).
- Manageable file count in OneLake (batched Parquet instead of one-per-event).
- We own the consumer loop — more code, but more control.
