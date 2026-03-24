# Open Mirroring Debezium

Streams CDC events from [Debezium](https://debezium.io/) via Azure EventHub into [Microsoft Fabric Open Mirroring](https://learn.microsoft.com/en-us/fabric/mirroring/open-mirroring) landing zones as Parquet files.

## What it does

```
Source DB → Debezium → Azure EventHub → This pipeline → Parquet → OneLake Landing Zone → Fabric Delta Tables
```

1. **Reads** batches of Debezium CDC events from EventHub
2. **Parses** the Debezium envelope (insert/update/delete/snapshot operations)
3. **Converts** to Parquet with Open Mirroring's `__rowMarker__` column
4. **Uploads** to the OneLake landing zone via ADLS Gen2 API
5. **Fabric** automatically ingests into Delta tables

## Supported Sources

| Source | Phase | Status |
|--------|-------|--------|
| Oracle (via Debezium) | 1 | 🟢 Active |
| SQL Server (via Debezium) | 2 | ⏳ Planned |

## Quick Start

```bash
# Install uv if you haven't
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync

# Run tests
uv run pytest

# Lint
uv run ruff check src/ tests/

# Type check
uv run pyright
```

## Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `EVENTHUB_CONNECTION_STRING` | EventHub connection string | Yes |
| `EVENTHUB_NAME` | EventHub name/topic | Yes |
| `EVENTHUB_CONSUMER_GROUP` | Consumer group (default: `$Default`) | No |
| `CHECKPOINT_BLOB_CONNECTION_STRING` | Blob storage for consumer checkpoints | Yes |
| `CHECKPOINT_BLOB_CONTAINER` | Blob container name | Yes |
| `ONELAKE_WORKSPACE_ID` | Fabric workspace GUID | Yes |
| `ONELAKE_MIRRORED_DB_ID` | Mirrored database GUID | Yes |
| `TABLE_CONFIG_PATH` | Path to `table_config.json` (default: `./table_config.json`) | No |
| `EVENTHUB_STARTING_POSITION` | Where to start reading when no checkpoint exists: `latest` (default, new events only), `earliest` (beginning of retention window), or a numeric sequence number. Ignored once a checkpoint is saved. | No |
| `SOURCE_TYPE` | Source database type for `_partnerEvents.json` (default: `Oracle`) | No |
| `FLUSH_MIN_RECORDS` | Minimum events per table before flushing to Parquet (default: `100`) | No |
| `FLUSH_MAX_RECORDS` | Maximum events per table before forced flush (default: `10000`) | No |
| `FLUSH_MIN_INTERVAL_SECONDS` | Minimum seconds between flushes per table (default: `1.0`) | No |
| `FLUSH_MAX_INTERVAL_SECONDS` | Maximum seconds before forced flush per table (default: `30.0`) | No |

### Table Config

See [`table_config.json`](table_config.json) for per-table key column overrides. New tables are auto-discovered from the Debezium event stream.

## Architecture

See [`docs/architecture.md`](docs/architecture.md) for the full system design.

## Documentation

- [Architecture](docs/architecture.md)
- [Design docs](docs/design/)
- [Decision records](docs/decisions/)
- [Local dev setup](docs/runbooks/local-dev-setup.md)
- [Oracle source notes](docs/sources/oracle.md)

## License

[MIT](LICENSE)
