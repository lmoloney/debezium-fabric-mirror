"""Per-table event buffer with configurable flush thresholds."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from .debezium_parser import ParsedEvent

logger = logging.getLogger(__name__)


@dataclass
class FlushConfig:
    """Flush threshold configuration."""

    min_records: int = 100
    max_records: int = 10_000
    min_interval_seconds: float = 1.0
    max_interval_seconds: float = 30.0


class EventBuffer:
    """Buffers ParsedEvents per table and flushes based on record count and time thresholds.

    Flush triggers:
    - A table has >= max_records → immediate flush
    - A table has >= min_records AND >= min_interval since last flush → flush
    - A table has any records AND >= max_interval since last flush → flush (latency bound)
    """

    def __init__(self, config: FlushConfig) -> None:
        self._config = config
        self._buffers: dict[str, list[ParsedEvent]] = {}
        self._last_flush: dict[str, float] = {}

    def add(self, table_key: str, events: list[ParsedEvent]) -> None:
        """Add events to a table's buffer."""
        if table_key not in self._buffers:
            self._buffers[table_key] = []
            self._last_flush.setdefault(table_key, time.monotonic())
        self._buffers[table_key].extend(events)

    def tables_ready(self) -> list[str]:
        """Return table keys that should be flushed now.

        A table is ready to flush when any of:
        - Buffer size >= max_records
        - Buffer size >= min_records AND time since last flush >= min_interval
        - Time since last flush >= max_interval (and buffer is non-empty)
        """
        now = time.monotonic()
        ready = []
        for table_key, events in self._buffers.items():
            if not events:
                continue
            count = len(events)
            elapsed = now - self._last_flush.get(table_key, now)

            if (
                count >= self._config.max_records
                or (count >= self._config.min_records and elapsed >= self._config.min_interval_seconds)
                or elapsed >= self._config.max_interval_seconds
            ):
                ready.append(table_key)
        return ready

    def flush(self, table_key: str) -> list[ParsedEvent]:
        """Drain and return buffered events for a table. Resets the flush timer."""
        events = self._buffers.pop(table_key, [])
        self._last_flush[table_key] = time.monotonic()
        if events:
            logger.debug("Flushed %d events for %s", len(events), table_key)
        return events

    def flush_all(self) -> dict[str, list[ParsedEvent]]:
        """Drain all non-empty buffers. Used at shutdown."""
        result: dict[str, list[ParsedEvent]] = {}
        for table_key in list(self._buffers.keys()):
            events = self._buffers.pop(table_key, [])
            if events:
                result[table_key] = events
                self._last_flush[table_key] = time.monotonic()
        return result

    @property
    def total_buffered(self) -> int:
        """Total number of events across all table buffers."""
        return sum(len(events) for events in self._buffers.values())

    @property
    def table_count(self) -> int:
        """Number of tables with buffered events."""
        return sum(1 for events in self._buffers.values() if events)
