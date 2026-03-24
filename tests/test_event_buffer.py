"""Tests for the per-table event buffer."""

from __future__ import annotations

from unittest.mock import patch

from open_mirroring_debezium.debezium_parser import ParsedEvent
from open_mirroring_debezium.event_buffer import EventBuffer, FlushConfig


def _make_event(
    id_val: float = 1.0,
    schema: str = "APPUSER",
    table: str = "SENSOR_READINGS",
    op: str = "c",
    row_marker: int = 0,
) -> ParsedEvent:
    """Build a minimal ParsedEvent for buffer tests."""
    return ParsedEvent(
        schema_name=schema,
        table_name=table,
        op=op,
        row_marker=row_marker,
        row_data={"ID": id_val, "__rowMarker__": row_marker},
        ts_ms=1773787238603,
    )


def _make_events(n: int, **kwargs) -> list[ParsedEvent]:
    """Build *n* events with sequential IDs."""
    return [_make_event(id_val=float(i), **kwargs) for i in range(n)]


TABLE_A = "APPUSER.SENSOR_READINGS"
TABLE_B = "APPUSER.ALERTS"


class TestAddAndBufferCount:
    def test_single_table(self):
        buf = EventBuffer(FlushConfig())
        buf.add(TABLE_A, _make_events(5))
        assert buf.total_buffered == 5

    def test_multiple_adds_same_table(self):
        buf = EventBuffer(FlushConfig())
        buf.add(TABLE_A, _make_events(3))
        buf.add(TABLE_A, _make_events(2))
        assert buf.total_buffered == 5

    def test_multiple_tables(self):
        buf = EventBuffer(FlushConfig())
        buf.add(TABLE_A, _make_events(3))
        buf.add(TABLE_B, _make_events(4))
        assert buf.total_buffered == 7


class TestMaxRecordsTrigger:
    def test_at_max_records(self):
        cfg = FlushConfig(max_records=10)
        buf = EventBuffer(cfg)
        buf.add(TABLE_A, _make_events(10))
        assert TABLE_A in buf.tables_ready()

    def test_above_max_records(self):
        cfg = FlushConfig(max_records=10)
        buf = EventBuffer(cfg)
        buf.add(TABLE_A, _make_events(15))
        assert TABLE_A in buf.tables_ready()


class TestMinRecordsPlusInterval:
    def test_ready_after_min_interval(self):
        cfg = FlushConfig(min_records=5, min_interval_seconds=2.0, max_interval_seconds=60.0)
        buf = EventBuffer(cfg)

        t = 100.0
        with patch("open_mirroring_debezium.event_buffer.time.monotonic", return_value=t):
            buf.add(TABLE_A, _make_events(5))

        with patch("open_mirroring_debezium.event_buffer.time.monotonic", return_value=t + 2.0):
            assert TABLE_A in buf.tables_ready()

    def test_not_ready_before_min_interval(self):
        cfg = FlushConfig(min_records=5, min_interval_seconds=2.0, max_interval_seconds=60.0)
        buf = EventBuffer(cfg)

        t = 100.0
        with patch("open_mirroring_debezium.event_buffer.time.monotonic", return_value=t):
            buf.add(TABLE_A, _make_events(5))

        with patch("open_mirroring_debezium.event_buffer.time.monotonic", return_value=t + 0.5):
            assert TABLE_A not in buf.tables_ready()


class TestMaxIntervalTrigger:
    def test_ready_after_max_interval_even_below_min_records(self):
        cfg = FlushConfig(min_records=100, max_interval_seconds=10.0)
        buf = EventBuffer(cfg)

        t = 100.0
        with patch("open_mirroring_debezium.event_buffer.time.monotonic", return_value=t):
            buf.add(TABLE_A, _make_events(3))

        with patch("open_mirroring_debezium.event_buffer.time.monotonic", return_value=t + 10.0):
            assert TABLE_A in buf.tables_ready()


class TestNotReadyBelowMinRecordsBeforeInterval:
    def test_not_ready(self):
        cfg = FlushConfig(min_records=100, max_interval_seconds=30.0)
        buf = EventBuffer(cfg)

        t = 100.0
        with patch("open_mirroring_debezium.event_buffer.time.monotonic", return_value=t):
            buf.add(TABLE_A, _make_events(5))

        with patch("open_mirroring_debezium.event_buffer.time.monotonic", return_value=t + 5.0):
            assert TABLE_A not in buf.tables_ready()


class TestFlushDrainsBuffer:
    def test_flush_returns_events(self):
        buf = EventBuffer(FlushConfig())
        events = _make_events(4)
        buf.add(TABLE_A, events)
        flushed = buf.flush(TABLE_A)
        assert flushed == events
        assert buf.total_buffered == 0

    def test_flush_nonexistent_table_returns_empty(self):
        buf = EventBuffer(FlushConfig())
        assert buf.flush("NONEXISTENT") == []


class TestFlushResetsTimer:
    def test_timer_resets_after_flush(self):
        cfg = FlushConfig(min_records=5, min_interval_seconds=2.0, max_interval_seconds=60.0)
        buf = EventBuffer(cfg)

        t = 100.0
        with patch("open_mirroring_debezium.event_buffer.time.monotonic", return_value=t):
            buf.add(TABLE_A, _make_events(10))

        # Flush resets the timer
        with patch("open_mirroring_debezium.event_buffer.time.monotonic", return_value=t + 3.0):
            buf.flush(TABLE_A)

        # Re-add events — timer was reset at t+3, so at t+4 only 1s has elapsed
        with patch("open_mirroring_debezium.event_buffer.time.monotonic", return_value=t + 3.0):
            buf.add(TABLE_A, _make_events(5))

        with patch("open_mirroring_debezium.event_buffer.time.monotonic", return_value=t + 4.0):
            assert TABLE_A not in buf.tables_ready()

        # After another 2s the interval is satisfied
        with patch("open_mirroring_debezium.event_buffer.time.monotonic", return_value=t + 5.0):
            assert TABLE_A in buf.tables_ready()


class TestFlushAll:
    def test_flush_all_returns_all_tables(self):
        buf = EventBuffer(FlushConfig())
        events_a = _make_events(3)
        events_b = _make_events(2)
        buf.add(TABLE_A, events_a)
        buf.add(TABLE_B, events_b)

        result = buf.flush_all()
        assert set(result.keys()) == {TABLE_A, TABLE_B}
        assert result[TABLE_A] == events_a
        assert result[TABLE_B] == events_b
        assert buf.total_buffered == 0


class TestFlushAllEmpty:
    def test_flush_all_empty(self):
        buf = EventBuffer(FlushConfig())
        assert buf.flush_all() == {}


class TestTablesReadyMultipleTables:
    def test_only_ready_tables_returned(self):
        cfg = FlushConfig(min_records=5, max_records=20, min_interval_seconds=2.0, max_interval_seconds=60.0)
        buf = EventBuffer(cfg)

        t = 100.0
        with patch("open_mirroring_debezium.event_buffer.time.monotonic", return_value=t):
            # TABLE_A: 25 events → exceeds max_records → ready
            buf.add(TABLE_A, _make_events(25))
            # TABLE_B: 2 events → below min_records, timer just started → not ready
            buf.add(TABLE_B, _make_events(2))

        with patch("open_mirroring_debezium.event_buffer.time.monotonic", return_value=t + 1.0):
            ready = buf.tables_ready()
            assert TABLE_A in ready
            assert TABLE_B not in ready
