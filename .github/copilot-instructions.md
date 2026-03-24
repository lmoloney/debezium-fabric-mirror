# Open Mirroring Debezium — Copilot Instructions

Python pipeline that reads Debezium CDC events from Azure EventHub, converts them to Parquet with Open Mirroring's `__rowMarker__` column, and uploads to Microsoft Fabric's OneLake landing zone.

## Build / Test / Lint Commands

```bash
uv sync                              # Install deps
uv run pytest                        # Run tests
uv run ruff check src/ tests/        # Lint
uv run ruff format src/ tests/       # Format
uv run pyright                       # Type check
docker build -t omd .                # Build container
uv run omd-consumer                  # Run the consumer locally
```

## Project Structure

```
src/open_mirroring_debezium/
├── __init__.py
├── debezium_parser.py    # Parse Debezium CDC JSON events
├── parquet_writer.py     # Build Parquet files with __rowMarker__
├── onelake_writer.py     # Upload to OneLake via ADLS Gen2
├── consumer.py           # EventHub batch consumer loop
└── config.py             # Table config + auto-discovery
```

## Module Responsibilities

- **debezium_parser**: Accepts raw EventHub message body (JSON with `schema` + `payload` wrapper from Debezium). Extracts table name, schema name, operation code, and row data. Maps Debezium op codes to Open Mirroring `__rowMarker__` values. Filters DDL/schema change events. Can extract primary keys from DDL events.
- **parquet_writer**: Groups parsed events by (schema, table). Builds PyArrow tables with `__rowMarker__` as the LAST column. Writes to in-memory buffers with Snappy compression. Handles type coercion (Oracle NUMBER→int64, Timestamp ms→timestamp[ms]).
- **onelake_writer**: Uploads Parquet buffers to OneLake landing zone via `azure-storage-file-datalake`. Uses `DefaultAzureCredential`. Temp-file-then-rename for atomicity. Creates `_metadata.json` per table with key columns.
- **consumer**: Main entry point. Connects to EventHub with `receive_batch()`. Orchestrates parse→parquet→upload→checkpoint flow.
- **config**: Loads `table_config.json` + env vars. Two-tier: defaults + per-table overrides. Auto-discovers new tables.

## Key Conventions

- Source uses Debezium 2.7.3 with Oracle connector. `schemas.enable=true` (events have `{"schema": {...}, "payload": {...}}` wrapper).
- Op code mapping: `c`→0 (insert), `r`→0 (snapshot), `u`→4 (upsert), `d`→2 (delete).
- `__rowMarker__` MUST be the last column in Parquet files.
- All auth uses `DefaultAzureCredential` — service principal for local dev, managed identity in prod.
- OneLake landing zone uses `fileDetectionStrategy: LastUpdateTimeFileDetection` and `isUpsertDefaultRowMarker: true`.

## Decision Records

Decision records live in `docs/decisions/`. Lightweight format with front matter (id, title, status, date, tags). When making architectural decisions, create a new record numbered sequentially.

## Adding a New Source Connector

1. The pipeline is connector-agnostic — Debezium normalizes the event format.
2. Add sample events to `tests/sample_events/<connector>_*.json`.
3. Add connector-specific notes to `docs/sources/<connector>.md`.
4. Add type mapping notes to `docs/design/type-mapping.md` if needed.
5. Add table config overrides in `table_config.json`.
6. Create a decision record if architectural changes are needed.

## Testing

- Fixtures in `tests/sample_events/` — real EventHub captures from Oracle Debezium connector.
- Use `pytest` with `pytest-asyncio` for async tests.
- Marker `@pytest.mark.integration` for tests that need external services.

## Environment Variables

See README.md for the full list. For local dev, copy `.env.example` to `.env`.
