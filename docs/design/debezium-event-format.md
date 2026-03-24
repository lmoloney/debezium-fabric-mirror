# Debezium Event Format

Analysis based on real sample data from an Oracle Debezium 2.7.3.Final connector publishing to Azure EventHub.

## Event Envelope Structure

With `schemas.enable=true`, each EventHub message has this structure:

```json
{
  "partition": "3",
  "sequence": 49,
  "offset": "196072",
  "enqueued_time": "2026-03-17T22:40:39...",
  "body": {
    "schema": {
      "type": "struct",
      "name": "oracle-cdc.APPUSER.SENSOR_READINGS.Envelope",
      "fields": [ ... ]
    },
    "payload": {
      "before": null,
      "after": {
        "ID": 50.0,
        "SENSOR_ID": "SENSOR-001",
        "TEMPERATURE": 38.29,
        "HUMIDITY": 81.91,
        "PRESSURE": 1025.46,
        "STATUS": "WARNING",
        "READING_TS": 1773780181595,
        "UPDATED_AT": 1774298969760
      },
      "source": {
        "version": "2.7.3.Final",
        "connector": "oracle",
        "name": "oracle-cdc",
        "ts_ms": 1773787238603,
        "snapshot": "false",
        "db": "XEPDB1",
        "sequence": null,
        "schema": "APPUSER",
        "table": "SENSOR_READINGS",
        "txId": null,
        "scn": "4054421",
        "commit_scn": "4054422",
        "lcr_position": null
      },
      "op": "c",
      "ts_ms": 1773787238603,
      "transaction": null
    }
  },
  "system_properties": {
    "x-opt-sequence-number": 49,
    "x-opt-offset": "196072",
    "x-opt-enqueued-time": "2026-03-17T22:40:39..."
  }
}
```

### Key Fields

| Field | Location | Description |
|---|---|---|
| `payload.op` | `body.payload.op` | Operation code (see below) |
| `payload.before` | `body.payload.before` | Row state before the change (null for inserts) |
| `payload.after` | `body.payload.after` | Row state after the change (null for deletes) |
| `payload.source.schema` | `body.payload.source.schema` | Database schema name (e.g. `APPUSER`) |
| `payload.source.table` | `body.payload.source.table` | Table name (e.g. `SENSOR_READINGS`) |
| `payload.source.scn` | `body.payload.source.scn` | Oracle System Change Number |
| `payload.source.snapshot` | `body.payload.source.snapshot` | `"true"`, `"last"`, or `"false"` |
| `schema.name` | `body.schema.name` | Envelope name, encodes connector/schema/table |

## Operation Codes

| Code | Meaning | `before` | `after` | Description |
|---|---|---|---|---|
| `r` | Snapshot read | null | full row | Initial snapshot. `source.snapshot` = `"true"` or `"last"` |
| `c` | Create (insert) | null | full row | New row inserted |
| `u` | Update | full row | full row | Row updated. Both before and after images present |
| `d` | Delete | full row | null | Row deleted. Only before image present |

### Before/After Semantics

- **Insert (`c`) and snapshot (`r`)**: `before` is null, `after` contains the full row data. These are functionally identical for our purposes.
- **Update (`u`)**: Both `before` and `after` contain the full row. We use `after` for the row data and map to `__rowMarker__ = 4` (upsert).
- **Delete (`d`)**: `before` contains the full row data, `after` is null. We use `before` for the row data and map to `__rowMarker__ = 2` (delete).

## DDL / Schema Change Events

Schema change events have a **different structure** from DML events. They can be identified by:

1. No `op` field in the payload
2. Schema name contains `SchemaChangeValue` (e.g. `io.debezium.connector.oracle.SchemaChangeValue`)

### DDL Event Structure

```json
{
  "body": {
    "schema": {
      "name": "io.debezium.connector.oracle.SchemaChangeValue",
      ...
    },
    "payload": {
      "source": { ... },
      "databaseName": "XEPDB1",
      "schemaName": "APPUSER",
      "ddl": "CREATE TABLE \"APPUSER\".\"SENSOR_READINGS\" (\n  \"ID\" NUMBER GENERATED ALWAYS AS IDENTITY ...\n);",
      "tableChanges": [
        {
          "type": "CREATE",
          "id": "\"XEPDB1\".\"APPUSER\".\"SENSOR_READINGS\"",
          "table": {
            "defaultCharsetName": null,
            "primaryKeyColumnNames": ["ID"],
            "columns": [
              {
                "name": "ID",
                "jdbcType": 2,
                "typeName": "NUMBER",
                "typeExpression": "NUMBER",
                "charsetName": null,
                "length": 38,
                "scale": 0,
                "optional": false,
                "autoIncremented": true,
                "generated": true
              },
              ...
            ]
          }
        }
      ]
    }
  }
}
```

### Useful DDL Fields

| Field | Description |
|---|---|
| `payload.ddl` | Full DDL statement (CREATE TABLE, ALTER TABLE, etc.) |
| `payload.tableChanges[0].type` | `CREATE`, `ALTER`, or `DROP` |
| `payload.tableChanges[0].table.primaryKeyColumnNames` | Array of PK column names |
| `payload.tableChanges[0].table.columns` | Full column metadata (name, type, length, scale, optional, etc.) |

DDL events are used to bootstrap table metadata (column names, types, primary keys) when a table is first seen, and to detect schema evolution.

## Sample Column Data — SENSOR_READINGS

Real wire values from the Oracle connector:

| Column | Oracle Type | Debezium Wire Type | Sample Value | Notes |
|---|---|---|---|---|
| ID | NUMBER (identity) | double | `50.0` | Needs `int()` coercion for PK use |
| SENSOR_ID | VARCHAR2(20) | string | `"SENSOR-001"` | |
| TEMPERATURE | NUMBER(5,2) | double | `38.29` | |
| HUMIDITY | NUMBER(5,2) | double | `81.91` | |
| PRESSURE | NUMBER(7,2) | double | `1025.46` | |
| STATUS | VARCHAR2(20) | string | `"WARNING"` | |
| READING_TS | TIMESTAMP(6) | int64 | `1773780181595` | Milliseconds since epoch |
| UPDATED_AT | TIMESTAMP(6) | int64 | `1774298969760` | Milliseconds since epoch |

> **Note**: Oracle NUMBERs arrive as plain `double` values — no base64 or bytes encoding. This indicates `decimal.handling.mode` is set to `double` (or `float`). Timestamp columns arrive as `int64` (milliseconds since epoch) with the logical type `org.apache.kafka.connect.data.Timestamp`.
