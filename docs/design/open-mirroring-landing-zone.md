# Open Mirroring Landing Zone

Conventions and requirements for writing Parquet files to the Fabric Open Mirroring landing zone.

Reference: [Open Mirroring Landing Zone Format](https://learn.microsoft.com/en-us/fabric/mirroring/open-mirroring-landing-zone-format)

## Path Structure

```
https://onelake.dfs.fabric.microsoft.com/
  <workspace-id>/
    <mirrored-db-id>/
      Files/
        LandingZone/
          <SchemaName>.schema/
            <TableName>/
              _metadata.json
              <parquet-files>
```

Example for the SENSOR_READINGS table:

```
https://onelake.dfs.fabric.microsoft.com/
  aabbccdd-1234-.../
    eeffgghh-5678-.../
      Files/
        LandingZone/
          APPUSER.schema/
            SENSOR_READINGS/
              _metadata.json
              a1b2c3d4-e5f6-7890-abcd-ef1234567890.parquet
              f9e8d7c6-b5a4-3210-fedc-ba0987654321.parquet
```

## _metadata.json

Each table folder contains a `_metadata.json` file that tells Fabric how to process the Parquet files:

```json
{
  "keyColumns": ["ID"],
  "fileDetectionStrategy": "LastUpdateTimeFileDetection",
  "isUpsertDefaultRowMarker": true
}
```

| Field | Description |
|---|---|
| `keyColumns` | Primary key column(s) used for upsert/delete matching. Must match the source table PK. |
| `fileDetectionStrategy` | `LastUpdateTimeFileDetection` — files are processed in order of modification timestamp. No sequential naming required. |
| `isUpsertDefaultRowMarker` | When `true`, rows without an explicit `__rowMarker__` are treated as upserts. We always set `__rowMarker__` explicitly, but this is a safety net. |

## __rowMarker__ Column

The `__rowMarker__` column is **required** and **must be the last column** in the Parquet schema.

| Value | Meaning | When We Use It |
|---|---|---|
| 0 | Insert | Debezium `op: "r"` (snapshot) or `op: "c"` (create) |
| 1 | Update | — (not used; we use upsert instead) |
| 2 | Delete | Debezium `op: "d"` |
| 4 | Upsert | Debezium `op: "u"` (update) and as default for inserts |

We map Debezium operations to `__rowMarker__` values as follows:

| Debezium op | Row Data Source | __rowMarker__ |
|---|---|---|
| `r` (snapshot) | `payload.after` | 0 (insert) |
| `c` (create) | `payload.after` | 0 (insert) |
| `u` (update) | `payload.after` | 4 (upsert) |
| `d` (delete) | `payload.before` | 2 (delete) |

> **Why upsert (4) for updates instead of update (1)?** Upsert is more resilient — if an insert event was lost or the table was partially snapshotted, an upsert will insert-or-update correctly. A strict update (1) would fail silently if the row doesn't exist yet.

## File Conventions

### Naming

We use UUID-based file names: `<uuid4>.parquet`. With `LastUpdateTimeFileDetection`, Fabric reads files by modification timestamp — no sequential naming or numbering is required.

### Compression

Parquet files use **Snappy** compression (PyArrow default, good balance of speed and size).

### Batch Size

Each Parquet file contains one batch of events for a single table. A batch corresponds to the events received in one `receive_batch()` call that belong to the same table.

## Atomicity — Temp File + Rename

Fabric continuously scans the landing zone for new files. To prevent it from reading a partially-uploaded file:

1. Upload the Parquet data to a temp path: `<table-folder>/_<uuid>.parquet` (underscore prefix)
2. Rename to the final path: `<table-folder>/<uuid>.parquet`

The ADLS Gen2 rename operation is atomic. Fabric ignores files prefixed with `_` (by convention), so the file only becomes visible to ingestion after the rename.

## Column Operations

Open Mirroring handles schema evolution with these rules:

| Change | Behavior |
|---|---|
| **New column added** | Column appears automatically in the Delta table after new Parquet files include it |
| **Column removed** | Existing column stays in Delta table; new rows get NULL for that column |
| **Column type changed** | **Not supported** — requires dropping the table folder and recreating it |

For column type changes, the recovery procedure is:
1. Delete the table folder from the landing zone
2. Recreate `_metadata.json`
3. Re-snapshot the table (or let Debezium re-snapshot)
4. Fabric recreates the Delta table from the new files
