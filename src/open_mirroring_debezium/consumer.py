"""EventHub batch consumer — main entry point."""

from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from typing import TYPE_CHECKING

from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore

from .config import AppConfig, get_table_config, load_config, register_table_from_ddl
from .debezium_parser import ParsedDDL, ParsedEvent, parse_event
from .onelake_writer import OneLakeWriter
from .parquet_writer import build_parquet

if TYPE_CHECKING:
    from azure.eventhub import EventData
    from azure.eventhub.aio import PartitionContext

logger = logging.getLogger(__name__)


async def process_batch(
    partition_context: PartitionContext,
    events: list[EventData],
    config: AppConfig,
    writer: OneLakeWriter,
) -> None:
    """Parse a batch of EventHub messages, group by table, and upload Parquet files.

    Each table's parquet build + upload is isolated: a failure for one table
    does not block others.  The checkpoint is only updated when at least one
    table was successfully processed.
    """
    if not events:
        return

    partition_id: str = partition_context.partition_id or "?"
    parsed_by_table: dict[str, list[ParsedEvent]] = defaultdict(list)
    skipped_events: int = 0

    for event in events:
        try:
            body = json.loads(event.body_as_str())
            result = parse_event(body)

            if isinstance(result, ParsedDDL):
                register_table_from_ddl(result.schema_name, result.table_name, result.primary_key_columns)
                continue

            if isinstance(result, ParsedEvent):
                key = f"{result.schema_name}.{result.table_name}"
                parsed_by_table[key].append(result)
            else:
                skipped_events += 1

        except Exception:
            skipped_events += 1
            logger.exception("Failed to parse event on partition %s", partition_id)
            continue

    if skipped_events:
        logger.warning("Partition %s — skipped %d unparseable events", partition_id, skipped_events)

    tables_ok: int = 0
    tables_failed: int = 0

    for table_key, table_events in parsed_by_table.items():
        schema, table = table_key.split(".", 1)
        try:
            table_cfg = get_table_config(schema, table)
            writer.ensure_table(schema, table, table_cfg.key_columns)
            parquet_bytes = build_parquet(table_events, key_columns=table_cfg.key_columns)
            writer.upload_parquet(schema, table, parquet_bytes)
            tables_ok += 1
            logger.info(
                "Partition %s — uploaded %s (%d events)",
                partition_id,
                table_key,
                len(table_events),
            )
        except Exception:
            tables_failed += 1
            logger.exception(
                "Partition %s — FAILED to process table %s (%d events lost)",
                partition_id,
                table_key,
                len(table_events),
            )

    # Only checkpoint if at least some work succeeded
    if tables_ok > 0:
        await partition_context.update_checkpoint()
        logger.info(
            "Checkpoint updated — partition %s, %d events, %d tables ok, %d tables failed",
            partition_id,
            len(events),
            tables_ok,
            tables_failed,
        )
    elif parsed_by_table:
        logger.error(
            "Partition %s — all %d tables failed, checkpoint NOT updated (%d events)",
            partition_id,
            tables_failed,
            len(events),
        )


def main() -> None:
    """Entry point for the ``omd-consumer`` CLI command."""
    from dotenv import load_dotenv

    load_dotenv()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    )

    config = load_config()
    writer = OneLakeWriter(config.workspace_id, config.mirrored_db_id)

    checkpoint_store = BlobCheckpointStore.from_connection_string(
        config.checkpoint_blob_connection_string,
        config.checkpoint_blob_container,
    )
    client = EventHubConsumerClient.from_connection_string(
        config.eventhub_connection_string,
        consumer_group=config.eventhub_consumer_group,
        eventhub_name=config.eventhub_name,
        checkpoint_store=checkpoint_store,
    )

    async def on_event_batch(partition_context: PartitionContext, events: list[EventData]) -> None:
        await process_batch(partition_context, events, config, writer)

    async def run() -> None:
        starting_pos = config.resolved_starting_position
        logger.info(
            "Starting consumer (group=%s, hub=%s, starting_position=%s)",
            config.eventhub_consumer_group,
            config.eventhub_name,
            starting_pos,
        )
        async with client:
            await client.receive_batch(
                on_event_batch=on_event_batch,
                max_batch_size=500,
                max_wait_time=5,
                starting_position=starting_pos,
            )

    asyncio.run(run())


if __name__ == "__main__":
    main()
