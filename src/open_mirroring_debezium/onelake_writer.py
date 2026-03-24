"""Upload Parquet files to OneLake Open Mirroring landing zone."""

from __future__ import annotations

import contextlib
import json
import logging
import time
import uuid

from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

logger = logging.getLogger(__name__)

ONELAKE_URL = "https://onelake.dfs.fabric.microsoft.com"

# Retry configuration
_MAX_RETRIES = 3
_BACKOFF_BASE_SECONDS = 1.0
_RETRYABLE_STATUS_CODES = {429, 500, 503}


def _is_retryable(exc: Exception) -> bool:
    """Return True if the exception represents a transient failure worth retrying."""
    # Azure SDK HTTP errors expose a status_code attribute
    status_code = getattr(exc, "status_code", None)
    if status_code is not None and status_code in _RETRYABLE_STATUS_CODES:
        return True
    # Connection-level errors (no HTTP status)
    if isinstance(exc, (ConnectionError, TimeoutError, OSError)):
        return True
    # Azure SDK wraps connection errors — check nested cause
    cause = getattr(exc, "__cause__", None)
    return isinstance(cause, (ConnectionError, TimeoutError, OSError))


class OneLakeWriter:
    """Manages Parquet uploads and table metadata for the Open Mirroring landing zone."""

    def __init__(self, workspace_id: str, mirrored_db_id: str) -> None:
        credential = DefaultAzureCredential()
        self._service = DataLakeServiceClient(ONELAKE_URL, credential=credential)
        self._fs = self._service.get_file_system_client(workspace_id)
        self._db_id = mirrored_db_id
        self._initialized_tables: set[str] = set()
        self._partner_events_written: bool = False

    def ensure_partner_events(self, source_type: str = "Oracle") -> None:
        """Create ``_partnerEvents.json`` at the landing zone root if not already done.

        This file is recommended by Microsoft to identify the mirroring partner.
        Uses ``overwrite=True`` so concurrent instances won't fail.
        """
        if self._partner_events_written:
            return

        dir_client = self._fs.get_directory_client(f"{self._db_id}/Files/LandingZone")
        with contextlib.suppress(Exception):
            dir_client.create_directory()

        file_client = dir_client.get_file_client("_partnerEvents.json")
        content = json.dumps(
            {
                "partnerName": "OpenMirroringDebezium",
                "sourceInfo": {
                    "sourceType": source_type,
                    "sourceVersion": "",
                    "additionalInformation": {},
                },
            }
        ).encode()
        file_client.upload_data(content, overwrite=True)
        logger.info("Ensured _partnerEvents.json (sourceType=%s)", source_type)
        self._partner_events_written = True

    def _landing_path(self, schema: str, table: str) -> str:
        """Return the ADLS directory path for a given schema.table in the landing zone."""
        return f"{self._db_id}/Files/LandingZone/{schema}.schema/{table}"

    def ensure_table(self, schema: str, table: str, key_columns: list[str]) -> None:
        """Create ``_metadata.json`` in the landing zone if not already done for this table.

        Uses ``overwrite=True`` so concurrent instances racing to create the
        file won't fail — last writer wins with identical content.
        """
        table_key = f"{schema}.{table}"
        if table_key in self._initialized_tables:
            return

        dir_client = self._fs.get_directory_client(self._landing_path(schema, table))
        with contextlib.suppress(Exception):
            dir_client.create_directory()

        meta_client = dir_client.get_file_client("_metadata.json")
        metadata = json.dumps(
            {
                "keyColumns": key_columns,
                "fileDetectionStrategy": "LastUpdateTimeFileDetection",
                "isUpsertDefaultRowMarker": True,
            }
        ).encode()
        # Always overwrite — safe for concurrent instances writing identical metadata
        meta_client.upload_data(metadata, overwrite=True)
        logger.info("Ensured _metadata.json for %s (keys=%s)", table_key, key_columns)

        self._initialized_tables.add(table_key)

    def upload_parquet(self, schema: str, table: str, data: bytes) -> str:
        """Upload Parquet bytes to the landing zone with retry on transient failures.

        Retries up to ``_MAX_RETRIES`` times with exponential backoff on
        HTTP 429/500/503 and connection errors.
        Returns the final file name.
        """
        path = self._landing_path(schema, table)
        dir_client = self._fs.get_directory_client(path)
        file_name = f"{uuid.uuid4().hex}.parquet"

        last_exc: Exception | None = None
        for attempt in range(_MAX_RETRIES):
            try:
                file_client = dir_client.get_file_client(file_name)
                file_client.upload_data(data, overwrite=True)
                logger.info("Uploaded %s to %s (%d bytes)", file_name, path, len(data))
                return file_name
            except Exception as exc:
                last_exc = exc
                if attempt < _MAX_RETRIES - 1 and _is_retryable(exc):
                    delay = _BACKOFF_BASE_SECONDS * (2**attempt)
                    logger.warning(
                        "Transient error uploading to %s (attempt %d/%d, retrying in %.1fs): %s",
                        path,
                        attempt + 1,
                        _MAX_RETRIES,
                        delay,
                        exc,
                    )
                    time.sleep(delay)
                else:
                    raise

        raise last_exc  # type: ignore[misc]
