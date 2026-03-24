"""Configuration and table auto-discovery."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class TableConfig:
    """Resolved configuration for a single table."""

    key_columns: list[str] = field(default_factory=list)
    use_upsert: bool = True
    file_detection: str = "LastUpdateTimeFileDetection"


@dataclass
class AppConfig:
    """Top-level application configuration loaded from environment variables."""

    eventhub_connection_string: str
    eventhub_name: str
    eventhub_consumer_group: str
    checkpoint_blob_connection_string: str
    checkpoint_blob_container: str
    workspace_id: str
    mirrored_db_id: str
    table_config_path: str
    source_type: str = "Oracle"
    eventhub_starting_position: str = "latest"
    flush_min_records: int = 100
    flush_max_records: int = 10_000
    flush_min_interval_seconds: float = 1.0
    flush_max_interval_seconds: float = 30.0

    @property
    def resolved_starting_position(self) -> str | int:
        """Map the user-facing value to what the EventHub SDK expects.

        - ``"latest"``   → ``"@latest"`` (tail of stream)
        - ``"earliest"`` → ``"-1"`` (beginning of retention window)
        - A numeric string → the integer sequence number
        """
        val = self.eventhub_starting_position.strip().lower()
        if val == "latest":
            return "@latest"
        if val == "earliest":
            return "-1"
        try:
            return int(val)
        except ValueError:
            logger.warning(
                "Unrecognised EVENTHUB_STARTING_POSITION '%s' — defaulting to @latest",
                self.eventhub_starting_position,
            )
            return "@latest"


# ---------------------------------------------------------------------------
# Module-level state — populated by load_config()
# ---------------------------------------------------------------------------

_defaults: dict[str, Any] = {}
_overrides: dict[str, dict[str, Any]] = {}
_discovered_tables: dict[str, TableConfig] = {}


def load_config() -> AppConfig:
    """Load environment variables and table_config.json.  Returns *AppConfig*."""
    global _defaults, _overrides

    config = AppConfig(
        eventhub_connection_string=os.environ["EVENTHUB_CONNECTION_STRING"],
        eventhub_name=os.environ["EVENTHUB_NAME"],
        eventhub_consumer_group=os.environ.get("EVENTHUB_CONSUMER_GROUP", "$Default"),
        checkpoint_blob_connection_string=os.environ["CHECKPOINT_BLOB_CONNECTION_STRING"],
        checkpoint_blob_container=os.environ["CHECKPOINT_BLOB_CONTAINER"],
        workspace_id=os.environ["ONELAKE_WORKSPACE_ID"],
        mirrored_db_id=os.environ["ONELAKE_MIRRORED_DB_ID"],
        table_config_path=os.environ.get("TABLE_CONFIG_PATH", "./table_config.json"),
        source_type=os.environ.get("SOURCE_TYPE", "Oracle"),
        eventhub_starting_position=os.environ.get("EVENTHUB_STARTING_POSITION", "latest"),
        flush_min_records=int(os.environ.get("FLUSH_MIN_RECORDS", "100")),
        flush_max_records=int(os.environ.get("FLUSH_MAX_RECORDS", "10000")),
        flush_min_interval_seconds=float(os.environ.get("FLUSH_MIN_INTERVAL_SECONDS", "1.0")),
        flush_max_interval_seconds=float(os.environ.get("FLUSH_MAX_INTERVAL_SECONDS", "30.0")),
    )

    table_cfg_path = Path(config.table_config_path)
    if table_cfg_path.exists():
        with open(table_cfg_path) as fh:
            raw = json.load(fh)
        _defaults = raw.get("defaults", {})
        _overrides = raw.get("overrides", {})
        logger.info("Loaded table config from %s (%d overrides)", table_cfg_path, len(_overrides))
    else:
        logger.warning("Table config not found at %s — using built-in defaults", table_cfg_path)
        _defaults = {}
        _overrides = {}

    return config


def get_table_config(schema: str, table: str) -> TableConfig:
    """Return the merged configuration for *schema.table*.

    Resolution order: per-table override → file defaults → hard-coded defaults.
    Runtime-discovered tables (via DDL events) are also consulted.
    """
    table_key = f"{schema}.{table}"

    # Start from hard-coded defaults
    merged = TableConfig()

    # Layer file-level defaults
    if "use_upsert" in _defaults:
        merged.use_upsert = bool(_defaults["use_upsert"])
    if "file_detection" in _defaults:
        merged.file_detection = str(_defaults["file_detection"])

    # Layer per-table overrides from config file
    override = _overrides.get(table_key, {})
    if "key_columns" in override:
        merged.key_columns = list(override["key_columns"])
    if "use_upsert" in override:
        merged.use_upsert = bool(override["use_upsert"])
    if "file_detection" in override:
        merged.file_detection = str(override["file_detection"])

    # Layer runtime-discovered tables (DDL events may supply key columns)
    if table_key in _discovered_tables:
        discovered = _discovered_tables[table_key]
        if discovered.key_columns and not merged.key_columns:
            merged.key_columns = discovered.key_columns

    return merged


def register_table_from_ddl(schema: str, table: str, primary_keys: list[str]) -> None:
    """Register a table discovered at runtime via a Debezium DDL event."""
    table_key = f"{schema}.{table}"
    _discovered_tables[table_key] = TableConfig(key_columns=primary_keys)
    logger.info("Registered table from DDL: %s (keys=%s)", table_key, primary_keys)
