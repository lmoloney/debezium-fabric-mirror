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
from .event_buffer import EventBuffer, FlushConfig
from .onelake_writer import OneLakeWriter
from .parquet_writer import build_parquet

if TYPE_CHECKING:
    from azure.eventhub import EventData
    from azure.eventhub.aio import PartitionContext

logger = logging.getLogger(__name__)


def flush_tables(
    table_keys: list[str],
    buffer: EventBuffer,
    writer: OneLakeWriter,
) -> tuple[int, int]:
    """Flush buffered events for the given tables: build parquet and upload.

    Returns (tables_ok, tables_failed).
    """
    tables_ok = 0
    tables_failed = 0

    for table_key in table_keys:
        events = buffer.flush(table_key)
        if not events:
            continue
        schema, table = table_key.split(".", 1)
        try:
            table_cfg = get_table_config(schema, table)
            writer.ensure_table(schema, table, table_cfg.key_columns)
            parquet_bytes = build_parquet(events, key_columns=table_cfg.key_columns)
            writer.upload_parquet(schema, table, parquet_bytes)
            tables_ok += 1
            logger.info("Flushed %s (%d events)", table_key, len(events))
        except Exception:
            tables_failed += 1
            logger.exception(
                "FAILED to flush table %s (%d events lost)",
                table_key,
                len(events),
            )

    return tables_ok, tables_failed


async def process_batch(
    partition_context: PartitionContext,
    events: list[EventData],
    config: AppConfig,
    writer: OneLakeWriter,
    buffer: EventBuffer,
) -> None:
    """Parse a batch of EventHub messages, buffer by table, and flush when thresholds are met.

    Checkpoint is only advanced when data has been successfully flushed to
    OneLake.  Events that are buffered but not yet flushed are intentionally
    *not* checkpointed — on restart they will be re-delivered by EventHub
    (at-least-once) and our upsert semantics make this safe.
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

    # Add parsed events to the buffer
    for table_key, table_events in parsed_by_table.items():
        buffer.add(table_key, table_events)

    # Check which tables are ready to flush
    ready = buffer.tables_ready()
    if not ready:
        return

    tables_ok, tables_failed = flush_tables(ready, buffer, writer)

    if tables_ok > 0:
        await partition_context.update_checkpoint()
        logger.info(
            "Checkpoint updated — partition %s, %d events, %d tables flushed, %d tables failed",
            partition_id,
            len(events),
            tables_ok,
            tables_failed,
        )
    elif tables_failed > 0:
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
    writer.ensure_partner_events(config.source_type)

    flush_config = FlushConfig(
        min_records=config.flush_min_records,
        max_records=config.flush_max_records,
        min_interval_seconds=config.flush_min_interval_seconds,
        max_interval_seconds=config.flush_max_interval_seconds,
    )
    buffer = EventBuffer(flush_config)

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
        await process_batch(partition_context, events, config, writer, buffer)

    async def run() -> None:
        starting_pos = config.resolved_starting_position
        logger.info(
            "Starting consumer (group=%s, hub=%s, starting_position=%s)",
            config.eventhub_consumer_group,
            config.eventhub_name,
            starting_pos,
        )
        try:
            async with client:
                await client.receive_batch(
                    on_event_batch=on_event_batch,
                    max_batch_size=500,
                    max_wait_time=5,
                    starting_position=starting_pos,
                )
        finally:
            # Flush any remaining buffered events on shutdown
            remaining = buffer.flush_all()
            if remaining:
                logger.info("Shutdown: flushing %d remaining tables", len(remaining))
                for table_key, events_list in remaining.items():
                    schema, table = table_key.split(".", 1)
                    try:
                        table_cfg = get_table_config(schema, table)
                        writer.ensure_table(schema, table, table_cfg.key_columns)
                        parquet_bytes = build_parquet(events_list, key_columns=table_cfg.key_columns)
                        writer.upload_parquet(schema, table, parquet_bytes)
                        logger.info("Shutdown flush: %s (%d events)", table_key, len(events_list))
                    except Exception:
                        logger.exception(
                            "Shutdown flush FAILED for %s (%d events lost)",
                            table_key,
                            len(events_list),
                        )

    asyncio.run(run())


if __name__ == "__main__":
    main()
