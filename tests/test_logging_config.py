"""Tests for logging configuration."""

import json
import logging
import re

import pytest

from open_mirroring_debezium.config import AppConfig
from open_mirroring_debezium.logging_config import JsonFormatter, configure_logging

_DUMMY_CONFIG_KWARGS = {
    "eventhub_connection_string": "fake",
    "eventhub_name": "fake",
    "eventhub_consumer_group": "$Default",
    "checkpoint_blob_connection_string": "fake",
    "checkpoint_blob_container": "fake",
    "workspace_id": "fake",
    "mirrored_db_id": "fake",
    "table_config_path": "fake",
}


@pytest.fixture()
def _cleanup_logging():
    """Remove handlers added to the root logger after each test."""
    yield
    root = logging.getLogger()
    root.handlers.clear()


# ---------------------------------------------------------------------------
# JsonFormatter — output
# ---------------------------------------------------------------------------


class TestJsonFormatterOutput:
    def test_produces_valid_json(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="hello", args=(), exc_info=None,
        )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert {"timestamp", "level", "logger", "message"} <= parsed.keys()

    def test_fields_match_record(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="my.logger", level=logging.WARNING, pathname="", lineno=0,
            msg="something happened", args=(), exc_info=None,
        )
        parsed = json.loads(formatter.format(record))
        assert parsed["level"] == "WARNING"
        assert parsed["logger"] == "my.logger"
        assert parsed["message"] == "something happened"


# ---------------------------------------------------------------------------
# JsonFormatter — exception handling
# ---------------------------------------------------------------------------


class TestJsonFormatterException:
    def test_includes_exception_traceback(self):
        formatter = JsonFormatter()
        try:
            raise ValueError("boom")
        except ValueError:
            import sys
            exc_info = sys.exc_info()

        record = logging.LogRecord(
            name="test", level=logging.ERROR, pathname="", lineno=0,
            msg="err", args=(), exc_info=exc_info,
        )
        parsed = json.loads(formatter.format(record))
        assert "exception" in parsed
        assert "boom" in parsed["exception"]


# ---------------------------------------------------------------------------
# JsonFormatter — timestamp format
# ---------------------------------------------------------------------------


class TestJsonFormatterTimestamp:
    def test_iso8601_utc_format(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="ts", args=(), exc_info=None,
        )
        parsed = json.loads(formatter.format(record))
        ts = parsed["timestamp"]
        # Expected: YYYY-MM-DDTHH:MM:SS.mmmZ
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", ts), (
            f"Unexpected timestamp format: {ts}"
        )


# ---------------------------------------------------------------------------
# configure_logging — defaults
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_cleanup_logging")
class TestConfigureLoggingDefaults:
    def test_app_logger_info(self):
        config = AppConfig(**_DUMMY_CONFIG_KWARGS)
        configure_logging(config)
        assert logging.getLogger("open_mirroring_debezium").level == logging.INFO

    def test_azure_logger_warning(self):
        config = AppConfig(**_DUMMY_CONFIG_KWARGS)
        configure_logging(config)
        assert logging.getLogger("azure").level == logging.WARNING

    def test_text_formatter_used(self):
        config = AppConfig(**_DUMMY_CONFIG_KWARGS)
        configure_logging(config)
        handler = logging.getLogger().handlers[0]
        assert not isinstance(handler.formatter, JsonFormatter)


# ---------------------------------------------------------------------------
# configure_logging — custom levels
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_cleanup_logging")
class TestConfigureLoggingCustomLevels:
    def test_custom_app_level(self):
        config = AppConfig(**_DUMMY_CONFIG_KWARGS, log_level="DEBUG")
        configure_logging(config)
        assert logging.getLogger("open_mirroring_debezium").level == logging.DEBUG

    def test_custom_azure_level(self):
        config = AppConfig(**_DUMMY_CONFIG_KWARGS, azure_log_level="ERROR")
        configure_logging(config)
        assert logging.getLogger("azure").level == logging.ERROR


# ---------------------------------------------------------------------------
# configure_logging — JSON format selection
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_cleanup_logging")
class TestConfigureLoggingJsonFormat:
    def test_json_formatter_selected(self):
        config = AppConfig(**_DUMMY_CONFIG_KWARGS, log_format="json")
        configure_logging(config)
        handler = logging.getLogger().handlers[0]
        assert isinstance(handler.formatter, JsonFormatter)
