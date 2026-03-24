"""Tests for the OneLake writer."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from open_mirroring_debezium.onelake_writer import OneLakeWriter


@pytest.fixture
def writer():
    """Create an OneLakeWriter with fully mocked Azure clients."""
    with (
        patch("open_mirroring_debezium.onelake_writer.DefaultAzureCredential"),
        patch("open_mirroring_debezium.onelake_writer.DataLakeServiceClient") as mock_service_cls,
    ):
        mock_service = mock_service_cls.return_value
        mock_fs = mock_service.get_file_system_client.return_value
        mock_dir = mock_fs.get_directory_client.return_value
        mock_file = mock_dir.get_file_client.return_value

        w = OneLakeWriter("workspace-id", "db-id")
        w._mock_fs = mock_fs  # type: ignore[attr-defined]
        w._mock_dir = mock_dir  # type: ignore[attr-defined]
        w._mock_file = mock_file  # type: ignore[attr-defined]
        yield w


class TestUploadParquet:
    def test_upload_calls_upload_data(self, writer: OneLakeWriter):
        data = b"parquet-bytes"
        writer.upload_parquet("APPUSER", "SENSOR_READINGS", data)
        writer._mock_file.upload_data.assert_called_once_with(data, overwrite=True)  # type: ignore[attr-defined]


class TestUploadParquetReturnsFilename:
    def test_returns_parquet_filename(self, writer: OneLakeWriter):
        result = writer.upload_parquet("APPUSER", "SENSOR_READINGS", b"data")
        assert result.endswith(".parquet")


class TestUploadParquetRetryOnTransient:
    @patch("open_mirroring_debezium.onelake_writer.time.sleep")
    def test_retries_on_503(self, mock_sleep: MagicMock, writer: OneLakeWriter):
        transient = Exception("Service Unavailable")
        transient.status_code = 503  # type: ignore[attr-defined]

        writer._mock_file.upload_data.side_effect = [transient, None]  # type: ignore[attr-defined]

        result = writer.upload_parquet("APPUSER", "SENSOR_READINGS", b"data")
        assert result.endswith(".parquet")
        assert writer._mock_file.upload_data.call_count == 2  # type: ignore[attr-defined]
        mock_sleep.assert_called_once()


class TestUploadParquetNoRetryOnPermanent:
    @patch("open_mirroring_debezium.onelake_writer.time.sleep")
    def test_raises_immediately_on_404(self, mock_sleep: MagicMock, writer: OneLakeWriter):
        permanent = Exception("Not Found")
        permanent.status_code = 404  # type: ignore[attr-defined]

        writer._mock_file.upload_data.side_effect = permanent  # type: ignore[attr-defined]

        with pytest.raises(Exception, match="Not Found"):
            writer.upload_parquet("APPUSER", "SENSOR_READINGS", b"data")

        assert writer._mock_file.upload_data.call_count == 1  # type: ignore[attr-defined]
        mock_sleep.assert_not_called()


class TestEnsureTableCreatesMetadata:
    def test_creates_metadata_json(self, writer: OneLakeWriter):
        writer.ensure_table("APPUSER", "SENSOR_READINGS", ["ID"])
        writer._mock_file.upload_data.assert_called_once()  # type: ignore[attr-defined]

        raw = writer._mock_file.upload_data.call_args[0][0]  # type: ignore[attr-defined]
        meta = json.loads(raw)
        assert meta["keyColumns"] == ["ID"]
        assert meta["fileDetectionStrategy"] == "LastUpdateTimeFileDetection"
        assert meta["isUpsertDefaultRowMarker"] is True


class TestEnsureTableIdempotent:
    def test_second_call_is_noop(self, writer: OneLakeWriter):
        writer.ensure_table("APPUSER", "SENSOR_READINGS", ["ID"])
        writer.ensure_table("APPUSER", "SENSOR_READINGS", ["ID"])
        writer._mock_file.upload_data.assert_called_once()  # type: ignore[attr-defined]


class TestEnsurePartnerEvents:
    def test_creates_partner_events_json(self, writer: OneLakeWriter):
        writer.ensure_partner_events("Oracle")
        writer._mock_file.upload_data.assert_called_once()  # type: ignore[attr-defined]

        raw = writer._mock_file.upload_data.call_args[0][0]  # type: ignore[attr-defined]
        content = json.loads(raw)
        assert content["partnerName"] == "OpenMirroringDebezium"
        assert content["sourceInfo"]["sourceType"] == "Oracle"


class TestEnsurePartnerEventsIdempotent:
    def test_second_call_is_noop(self, writer: OneLakeWriter):
        writer.ensure_partner_events("Oracle")
        writer.ensure_partner_events("Oracle")
        writer._mock_file.upload_data.assert_called_once()  # type: ignore[attr-defined]
