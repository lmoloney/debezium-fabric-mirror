"""Tests for the Parquet writer."""

import io

import pyarrow.parquet as pq

from open_mirroring_debezium.debezium_parser import ParsedEvent
from open_mirroring_debezium.parquet_writer import build_parquet


def _make_event(
    id_val: float,
    sensor_id: str = "SENSOR-001",
    temperature: float = 22.5,
    humidity: float = 45.0,
    pressure: float | None = 1013.25,
    status: str = "ACTIVE",
    op: str = "c",
    row_marker: int = 0,
) -> ParsedEvent:
    """Build a realistic ParsedEvent for SENSOR_READINGS."""
    row_data = {
        "ID": id_val,
        "SENSOR_ID": sensor_id,
        "TEMPERATURE": temperature,
        "HUMIDITY": humidity,
        "PRESSURE": pressure,
        "STATUS": status,
        "READING_TS": 1773787238603000,
        "UPDATED_AT": 1773787238603000,
        "__rowMarker__": row_marker,
    }
    return ParsedEvent(
        schema_name="APPUSER",
        table_name="SENSOR_READINGS",
        op=op,
        row_marker=row_marker,
        row_data=row_data,
        ts_ms=1773787238603,
    )


class TestBuildParquetBasic:
    def test_build_parquet_basic(self):
        events = [_make_event(1.0), _make_event(2.0), _make_event(3.0)]
        data = build_parquet(events)
        assert isinstance(data, bytes)
        assert len(data) > 0
        # Valid Parquet: readable by pyarrow
        table = pq.read_table(io.BytesIO(data))
        assert table.num_rows == 3

    def test_columns_present(self):
        events = [_make_event(1.0)]
        data = build_parquet(events)
        table = pq.read_table(io.BytesIO(data))
        col_names = table.column_names
        assert "ID" in col_names
        assert "SENSOR_ID" in col_names
        assert "TEMPERATURE" in col_names
        assert "__rowMarker__" in col_names


class TestRowMarkerLastColumn:
    def test_row_marker_last_column(self):
        events = [_make_event(1.0)]
        data = build_parquet(events)
        table = pq.read_table(io.BytesIO(data))
        assert table.column_names[-1] == "__rowMarker__"


class TestPreservesRowOrder:
    def test_preserves_row_order(self):
        events = [_make_event(10.0), _make_event(20.0), _make_event(30.0)]
        data = build_parquet(events)
        table = pq.read_table(io.BytesIO(data))
        ids = table.column("ID").to_pylist()
        assert ids == [10.0, 20.0, 30.0]


class TestHandlesNullValues:
    def test_handles_null_values(self):
        event = _make_event(1.0, pressure=None)
        data = build_parquet([event])
        table = pq.read_table(io.BytesIO(data))
        assert table.num_rows == 1
        pressure_vals = table.column("PRESSURE").to_pylist()
        assert pressure_vals == [None]


class TestSnappyCompression:
    def test_snappy_compression(self):
        events = [_make_event(1.0)]
        data = build_parquet(events)
        pf = pq.ParquetFile(io.BytesIO(data))
        row_group = pf.metadata.row_group(0)
        # Check at least one column uses snappy
        compressions = {row_group.column(i).compression for i in range(row_group.num_columns)}
        assert "SNAPPY" in compressions
