"""Tests for the Debezium event parser."""

from open_mirroring_debezium.debezium_parser import ParsedDDL, ParsedEvent, parse_event


class TestParseSnapshot:
    def test_parse_snapshot(self, oracle_snapshot):
        result = parse_event(oracle_snapshot)
        assert isinstance(result, ParsedEvent)
        assert result.op == "r"
        assert result.row_marker == 0
        assert result.schema_name == "APPUSER"
        assert result.table_name == "SENSOR_READINGS"
        assert result.row_data["ID"] == 50.0

    def test_snapshot_uses_after(self, oracle_snapshot):
        result = parse_event(oracle_snapshot)
        assert isinstance(result, ParsedEvent)
        assert "TEMPERATURE" in result.row_data
        assert "HUMIDITY" in result.row_data


class TestParseInsert:
    def test_parse_insert(self, oracle_insert):
        result = parse_event(oracle_insert)
        assert isinstance(result, ParsedEvent)
        assert result.op == "c"
        assert result.row_marker == 0
        assert result.schema_name == "APPUSER"
        assert result.table_name == "SENSOR_READINGS"
        assert result.row_data["ID"] == 61.0


class TestParseUpdate:
    def test_parse_update(self, oracle_update):
        result = parse_event(oracle_update)
        assert isinstance(result, ParsedEvent)
        assert result.op == "u"
        assert result.row_marker == 4
        # Update uses "after" for row data
        assert result.row_data["ID"] == 2.0

    def test_update_has_after_data(self, oracle_update):
        result = parse_event(oracle_update)
        assert isinstance(result, ParsedEvent)
        assert "TEMPERATURE" in result.row_data


class TestParseDelete:
    def test_parse_delete(self, oracle_delete):
        result = parse_event(oracle_delete)
        assert isinstance(result, ParsedEvent)
        assert result.op == "d"
        assert result.row_marker == 2
        # Delete uses "before" for row data
        assert result.row_data["ID"] == 22.0


class TestParseDDL:
    def test_parse_ddl(self, oracle_ddl):
        result = parse_event(oracle_ddl)
        assert isinstance(result, ParsedDDL)
        assert result.primary_key_columns == ["ID"]
        assert "CREATE TABLE" in result.ddl_text

    def test_ddl_schema_table(self, oracle_ddl):
        result = parse_event(oracle_ddl)
        assert isinstance(result, ParsedDDL)
        assert result.schema_name == "APPUSER"
        assert result.table_name == "SENSOR_READINGS"

    def test_ddl_columns(self, oracle_ddl):
        result = parse_event(oracle_ddl)
        assert isinstance(result, ParsedDDL)
        col_names = [c["name"] for c in result.columns]
        assert "ID" in col_names
        assert "TEMPERATURE" in col_names
        assert len(result.columns) == 8


class TestRowMarkerInRowData:
    def test_row_marker_in_row_data(self, oracle_insert):
        result = parse_event(oracle_insert)
        assert isinstance(result, ParsedEvent)
        assert "__rowMarker__" in result.row_data
        assert result.row_data["__rowMarker__"] == result.row_marker


class TestSchemaTableExtraction:
    def test_schema_table_extraction(self, oracle_snapshot):
        result = parse_event(oracle_snapshot)
        assert isinstance(result, ParsedEvent)
        assert result.schema_name == "APPUSER"
        assert result.table_name == "SENSOR_READINGS"

    def test_all_dml_events_same_table(self, oracle_snapshot, oracle_insert, oracle_update, oracle_delete):
        for body in [oracle_snapshot, oracle_insert, oracle_update, oracle_delete]:
            result = parse_event(body)
            assert isinstance(result, ParsedEvent)
            assert result.schema_name == "APPUSER"
            assert result.table_name == "SENSOR_READINGS"
