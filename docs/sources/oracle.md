# Oracle Source Connector

Specifics for the Debezium Oracle connector used in Phase 1.

## Connector Details

| Setting | Value |
|---|---|
| Debezium version | 2.7.3.Final |
| Connector class | `io.debezium.connector.oracle.OracleConnector` |
| Change capture method | LogMiner |
| Database | XEPDB1 (Oracle XE pluggable database) |
| Schema | APPUSER |
| Connector name / server name | `oracle-cdc` |

## Tables

### Phase 1

| Table | Description |
|---|---|
| `APPUSER.SENSOR_READINGS` | IoT sensor data — temperature, humidity, pressure readings |

### SENSOR_READINGS Schema

```sql
CREATE TABLE "APPUSER"."SENSOR_READINGS" (
    "ID"          NUMBER GENERATED ALWAYS AS IDENTITY,
    "SENSOR_ID"   VARCHAR2(20),
    "TEMPERATURE" NUMBER(5,2),
    "HUMIDITY"    NUMBER(5,2),
    "PRESSURE"    NUMBER(7,2),
    "STATUS"      VARCHAR2(20),
    "READING_TS"  TIMESTAMP(6),
    "UPDATED_AT"  TIMESTAMP(6),
    PRIMARY KEY ("ID")
);
```

## Decimal Handling

`decimal.handling.mode` appears to be `double` (or `float`). Evidence:

- Oracle `NUMBER` columns arrive as plain JSON doubles (e.g. `38.29`, `50.0`)
- No base64-encoded byte arrays or struct wrappers
- Identity column `ID` arrives as `50.0` (a double), not as an integer or encoded decimal

This simplifies parsing — no need to decode `org.apache.kafka.connect.data.Decimal` structs. The trade-off is potential floating-point precision loss for very large or high-precision numbers, but this is acceptable for the Phase 1 data types.

## Timestamp Handling

Oracle `TIMESTAMP(6)` columns are mapped by Debezium to the `org.apache.kafka.connect.data.Timestamp` logical type:

- Wire format: `int64` (milliseconds since epoch)
- Example: `1773780181595` → `2026-03-15T14:23:01.595Z`
- Oracle stores microsecond precision, but Debezium's Timestamp logical type truncates to milliseconds

## Identity Columns

Oracle identity columns (`NUMBER GENERATED ALWAYS AS IDENTITY`) have a quirk:

- Source type: `NUMBER` (no explicit precision/scale)
- Wire type: `double`
- Sample value: `50.0`
- Required coercion: `int(value)` before writing as `pa.int64()`

The parser identifies identity/PK columns from:
1. DDL events (`tableChanges[0].table.primaryKeyColumnNames`)
2. `_metadata.json` key columns configuration
3. Explicit table configuration

## Snapshot Behavior

During initial snapshot or re-snapshot:

- Events use `op: "r"` (read)
- `source.snapshot` is `"true"` for all but the last event, which has `"last"`
- `before` is always null (snapshot reads have no prior state)
- Events arrive in table scan order (by PK)

After the snapshot completes, the connector switches to streaming mode and uses `op: "c"`, `"u"`, `"d"` for live changes.

## DDL Events

The Oracle connector emits DDL events for schema changes. These events:

- Have no `op` field
- Schema name contains `SchemaChangeValue`
- Include the full `CREATE TABLE` (or `ALTER TABLE`) DDL statement
- Include structured column metadata in `tableChanges[0].table.columns`
- Include primary key information in `tableChanges[0].table.primaryKeyColumnNames`

DDL events are emitted during snapshot (for each captured table) and when schema changes are detected during streaming.

## LogMiner Notes

- LogMiner is Oracle's built-in log mining utility for reading redo logs
- Requires `ARCHIVELOG` mode enabled on the database
- The connector user needs specific grants (`LOGMINING`, `SELECT ANY TRANSACTION`, etc.)
- LogMiner has higher latency than Oracle's XStream API but requires no additional Oracle licensing
- SCN (System Change Number) is used for position tracking — visible in `source.scn` and `source.commit_scn`
