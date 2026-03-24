# System Architecture

## Data Flow

```
┌──────────────┐     ┌─────────────────────────┐     ┌──────────────────┐
│  Source DB    │────▶│  Debezium (Kafka Connect)│────▶│  Azure EventHub  │
│  (Oracle XE)  │     │  Oracle LogMiner CDC     │     │  (topic/partitions)│
└──────────────┘     └─────────────────────────┘     └────────┬─────────┘
                                                               │
                                                               ▼
                                                   ┌───────────────────────┐
                                                   │  Python Container App  │
                                                   │  (azure-eventhub SDK)  │
                                                   │                       │
                                                   │  ┌─────────────────┐  │
                                                   │  │ receive_batch() │  │
                                                   │  └────────┬────────┘  │
                                                   │           ▼           │
                                                   │  ┌─────────────────┐  │
                                                   │  │ Debezium Parser │  │
                                                   │  │ (table, op, row)│  │
                                                   │  └────────┬────────┘  │
                                                   │           ▼           │
                                                   │  ┌─────────────────┐  │
                                                   │  │ Parquet Writer  │  │
                                                   │  │ (PyArrow, Snappy│  │
                                                   │  │  __rowMarker__) │  │
                                                   │  └────────┬────────┘  │
                                                   │           ▼           │
                                                   │  ┌─────────────────┐  │
                                                   │  │ OneLake Writer  │  │
                                                   │  │ (ADLS Gen2 API) │  │
                                                   │  │ temp→rename     │  │
                                                   │  └────────┬────────┘  │
                                                   └───────────┼───────────┘
                                                               ▼
                                              ┌─────────────────────────────────┐
                                              │  OneLake Landing Zone           │
                                              │  /<workspace>/<mirrored-db>/    │
                                              │   Files/LandingZone/            │
                                              │    <Schema>.schema/<Table>/     │
                                              └───────────────┬─────────────────┘
                                                              ▼
                                              ┌─────────────────────────────────┐
                                              │  Fabric Auto-Ingestion          │
                                              │  Landing Zone → Delta Tables    │
                                              └─────────────────────────────────┘
```

## Component Responsibilities

### Source Database (Oracle XE)

The upstream OLTP database running Oracle XE with a pluggable database (XEPDB1). Tables are owned by the `APPUSER` schema. LogMiner is enabled for CDC. The database has no awareness of the mirroring pipeline — Debezium reads the redo logs non-invasively.

### Debezium (Kafka Connect)

Debezium 2.7.3 runs as a Kafka Connect source connector using the Oracle LogMiner adapter. It reads the redo log, converts row-level changes into structured JSON envelopes (with `before`/`after`/`source`/`op` fields), and publishes them to Azure EventHub via the Kafka protocol. Schema change (DDL) events are also captured. The connector is configured with `schemas.enable=true` so each event includes its full schema definition.

### Azure EventHub

Acts as the durable message broker between Debezium and the Python consumer. Events are partitioned (Debezium routes by table or key). EventHub provides at-least-once delivery semantics and consumer group offset tracking. The consumer group lag metric is exposed for KEDA-based autoscaling.

### Python Container App

The core processing application, deployed as an Azure Container App. It runs a continuous loop calling `receive_batch()` from the `azure-eventhub` SDK to pull events from assigned partitions. Each batch is processed through the parser → writer → uploader pipeline. The container is stateless — all state lives in EventHub checkpoints and OneLake files.

### Debezium Parser

Extracts the table name, operation type (`r`/`c`/`u`/`d`), and row data from the Debezium envelope. Handles the asymmetry between insert events (`after` only), delete events (`before` only), and update events (both). DDL/schema-change events are detected and handled separately. The parser also extracts source metadata (schema name, table name, SCN) for routing and observability.

### Parquet Writer

Converts parsed row data into Parquet files using PyArrow. Applies type mappings (see [type-mapping.md](design/type-mapping.md)) and appends the required `__rowMarker__` column as the last column (values: 0=insert, 1=update, 2=delete, 4=upsert). Files are compressed with Snappy. Each file covers a batch of events for a single table.

### OneLake Writer

Uploads Parquet files to the Open Mirroring landing zone via the ADLS Gen2 REST API. Uses atomic writes: uploads to a temp file (prefixed with `_`), then renames to the final path. This prevents Fabric from reading partial files. Target path follows the convention: `/<workspace-id>/<mirrored-db-id>/Files/LandingZone/<Schema>.schema/<Table>/<uuid>.parquet`.

### Fabric Auto-Ingestion

Microsoft Fabric's built-in mirroring engine monitors the landing zone and automatically ingests new Parquet files into Delta tables. File detection uses `LastUpdateTimeFileDetection` — files are read in order of modification timestamp. The `__rowMarker__` column drives upsert/delete semantics in the Delta table.

## Authentication Model

All Azure service authentication uses `DefaultAzureCredential` from the `azure-identity` SDK. This provides a credential chain that works across environments:

| Environment | Credential Used |
|---|---|
| Local development | Azure CLI (`az login`) or VS Code |
| Container Apps | Managed Identity (system-assigned) |
| CI/CD | Workload Identity Federation or Service Principal |

The managed identity must have:
- **Azure Event Hubs Data Receiver** on the EventHub namespace
- **Storage Blob Data Contributor** on the OneLake endpoint (for ADLS Gen2 writes)

No connection strings or secrets are stored in application code or config.

## Scaling Model

The container app scales horizontally using KEDA (Kubernetes Event-Driven Autoscaling) based on EventHub consumer group lag:

```
Consumer Group Lag (unprocessed events)
    │
    ▼
KEDA EventHub Scaler
    │
    ▼
Scale 0..N container replicas
```

- **Scale-to-zero**: When no events are pending, the app scales to zero replicas.
- **Partition affinity**: Each replica processes a subset of EventHub partitions. EventHub's partition ownership protocol (via `BlobCheckpointStore`) ensures no two replicas process the same partition.
- **Max replicas**: Capped at the number of EventHub partitions (no benefit beyond 1:1).
- **Scale-up trigger**: Consumer group lag exceeds threshold (configurable).

## Error Handling Strategy

### Per-Partition Isolation

Each partition is processed independently. A failure in one partition does not block others. If processing fails for a partition, that partition's checkpoint is not advanced, so events will be retried on the next iteration.

### Retry Policy

Transient failures (network errors, throttling, temporary OneLake unavailability) are retried with exponential backoff. The retry budget is bounded to prevent infinite loops — after exhausting retries, the event batch is routed to dead-letter handling.

### Dead-Letter Handling

Events that cannot be processed after all retries are written to a dead-letter location (a separate container or OneLake path) with full context: the original event payload, error details, partition ID, offset, and timestamp. This allows manual inspection and replay without data loss.

### Idempotency

Parquet files are written with UUID-based names and atomic rename. Re-processing the same event batch produces a new file with different name — Fabric's upsert semantics (driven by `__rowMarker__` and key columns) ensure the Delta table converges to the correct state even if duplicates are ingested.
