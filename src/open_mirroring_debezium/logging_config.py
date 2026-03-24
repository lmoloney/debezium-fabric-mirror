"""Configurable logging with optional JSON output for container environments."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import AppConfig


class JsonFormatter(logging.Formatter):
    """Structured JSON log formatter for container environments."""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)  # noqa: UP017
        return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(record.msecs):03d}Z"

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, str] = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry)


def configure_logging(config: AppConfig) -> None:
    """Set up logging with separate levels for app and Azure SDK loggers."""
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # Remove any existing handlers (e.g., from basicConfig)
    root.handlers.clear()

    if config.log_format.lower() == "json":
        formatter: logging.Formatter = JsonFormatter()
    else:
        formatter = logging.Formatter("%(asctime)s %(levelname)-8s %(name)s — %(message)s")

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root.addHandler(handler)

    # App logger level
    app_logger = logging.getLogger("open_mirroring_debezium")
    app_logger.setLevel(getattr(logging, config.log_level.upper(), logging.INFO))

    # Azure SDK logger level — suppress the AMQP/HTTP noise
    azure_logger = logging.getLogger("azure")
    azure_logger.setLevel(getattr(logging, config.azure_log_level.upper(), logging.WARNING))

    # urllib3 noise comes from Azure SDK HTTP calls — same treatment
    urllib3_logger = logging.getLogger("urllib3")
    urllib3_logger.setLevel(getattr(logging, config.azure_log_level.upper(), logging.WARNING))

    # asyncio internals (selector, unclosed connections) — suppress below WARNING
    logging.getLogger("asyncio").setLevel(logging.WARNING)
