# Local Development Setup

## Prerequisites

- Python 3.12+
- uv (`curl -LsSf https://astral.sh/uv/install.sh | sh`)
- Azure CLI (`az`) for authentication
- Docker (for container builds)
- Access to: Azure EventHub namespace, Fabric workspace with a mirrored database

## Install Dependencies

```bash
uv sync
```

## Authentication Setup

This project uses `DefaultAzureCredential` which tries multiple auth methods in order.

### Option A: Azure CLI (simplest for dev)

```bash
az login
# DefaultAzureCredential will pick up your Azure CLI session
```

### Option B: Service Principal (for CI or shared dev)

Create a `.env` file (gitignored):

```
AZURE_TENANT_ID=your-tenant-id
AZURE_CLIENT_ID=your-client-id
AZURE_CLIENT_SECRET=your-client-secret
```

The service principal needs:

- **Contributor** role on the Fabric workspace (for OneLake access)
- **Azure Event Hubs Data Receiver** role on the EventHub namespace (if using managed identity for EventHub)

## Environment Variables

Create a `.env` file with:

```
EVENTHUB_CONNECTION_STRING=Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=...
EVENTHUB_NAME=your-debezium-topic
EVENTHUB_CONSUMER_GROUP=$Default
CHECKPOINT_BLOB_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=...
CHECKPOINT_BLOB_CONTAINER=checkpoints
ONELAKE_WORKSPACE_ID=your-workspace-guid
ONELAKE_MIRRORED_DB_ID=your-mirrored-db-guid
TABLE_CONFIG_PATH=./table_config.json
EVENTHUB_STARTING_POSITION=latest
```

> **Note:** `EVENTHUB_STARTING_POSITION` controls where the consumer starts reading when no checkpoint exists. Set to `earliest` to replay all retained events, `latest` (default) for new events only, or a numeric sequence number. Once a checkpoint is saved, the consumer always resumes from the checkpoint regardless of this setting.

## Running Locally

```bash
# Load env vars
export $(cat .env | xargs)

# Run the consumer
uv run omd-consumer
```

## Running Tests

```bash
# All tests
uv run pytest

# With verbose output
uv run pytest -v

# Only unit tests (skip integration)
uv run pytest -m "not integration"
```

## Lint and Format

```bash
uv run ruff check src/ tests/
uv run ruff format src/ tests/
uv run pyright
```

## Docker Build

```bash
docker build -t omd .
docker run --env-file .env omd
```

## Verifying OneLake Writes

After running, check the landing zone in Fabric:

1. Open your mirrored database in the Fabric portal
2. Check the landing zone URL on the Home page
3. Files should appear under `LandingZone/<Schema>.schema/<Table>/`
4. Monitor replication status in the mirrored database's monitoring tab

## Troubleshooting

### "No EventHub events received"

- Check `EVENTHUB_NAME` matches the EventHub/topic Debezium is writing to
- Verify the consumer group exists
- Check if another consumer is already processing events on the same consumer group
- If this is a first run with no checkpoints, the default starting position is `latest` (new events only). Set `EVENTHUB_STARTING_POSITION=earliest` to replay from the beginning of the retention window

### "Authentication failed"

- For Azure CLI: run `az login` and ensure correct subscription (`az account show`)
- For SPN: verify `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`
- Ensure the identity has Contributor on the Fabric workspace

### "OneLake upload failed (403)"

- The identity needs Contributor role on the Fabric workspace, not just the mirrored database
- Check the workspace ID and mirrored database ID are correct GUIDs
