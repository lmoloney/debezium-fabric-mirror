# Type Mapping

Debezium → PyArrow → Parquet type mappings for each supported source database.

## Oracle (Phase 1)

Confirmed from real Debezium 2.7.3 sample data against Oracle XE (XEPDB1).

| Debezium Wire Type | Debezium Logical Type | Oracle Source Type | PyArrow Type | Notes |
|---|---|---|---|---|
| double | — | NUMBER | `pa.float64()` | Default for Oracle NUMBER. `decimal.handling.mode` is `double`. |
| double | — | NUMBER (identity/PK) | `pa.int64()` | Coerce with `int(value)`. Identity PKs arrive as `50.0`; must be integers. |
| string | — | VARCHAR2 | `pa.string()` | No coercion needed. |
| int64 | `org.apache.kafka.connect.data.Timestamp` | TIMESTAMP(6) | `pa.timestamp('ms')` | Value is milliseconds since epoch. |
| int32 | — | — | `pa.int32()` | Used for the `__rowMarker__` column (synthetic, not from source). |

### Oracle Type Coercion Rules

1. **PK identity columns**: Oracle identity columns (`NUMBER GENERATED ALWAYS AS IDENTITY`) arrive as doubles (e.g. `50.0`). These must be coerced to `int` before writing as `pa.int64()`. The parser identifies PK columns from DDL events (`primaryKeyColumnNames`) or from configuration.

2. **Timestamps**: Oracle `TIMESTAMP(6)` columns have microsecond precision at the source but Debezium's Kafka Connect `Timestamp` logical type transmits milliseconds since epoch. We store as `pa.timestamp('ms')`.

3. **Plain NUMBERs**: Non-PK `NUMBER` columns (with or without precision/scale) stay as `pa.float64()`. This preserves the wire format without lossy conversions.

### Example: SENSOR_READINGS Row

```python
import pyarrow as pa

schema = pa.schema([
    pa.field("ID", pa.int64()),              # 50.0 → 50
    pa.field("SENSOR_ID", pa.string()),      # "SENSOR-001"
    pa.field("TEMPERATURE", pa.float64()),   # 38.29
    pa.field("HUMIDITY", pa.float64()),      # 81.91
    pa.field("PRESSURE", pa.float64()),      # 1025.46
    pa.field("STATUS", pa.string()),         # "WARNING"
    pa.field("READING_TS", pa.timestamp('ms')),  # 1773780181595
    pa.field("UPDATED_AT", pa.timestamp('ms')),  # 1774298969760
    pa.field("__rowMarker__", pa.int32()),   # 0 (insert)
])
```

## SQL Server (Phase 2)

Expected mappings — **to be confirmed with real sample data** when the SQL Server connector is set up.

| Debezium Wire Type | Debezium Logical Type | SQL Server Type | PyArrow Type | Notes |
|---|---|---|---|---|
| int32 | — | int | `pa.int32()` | Direct mapping. |
| int64 | — | bigint | `pa.int64()` | Direct mapping. |
| boolean | — | bit | `pa.bool_()` | `true`/`false`. |
| string | — | nvarchar, varchar | `pa.string()` | No coercion. |
| int64 | `org.apache.kafka.connect.data.Timestamp` | datetime2 | `pa.timestamp('ms')` | Same as Oracle — ms since epoch. |
| int32 | `org.apache.kafka.connect.data.Date` | date | `pa.date32()` | Days since epoch. |
| struct (bytes) | `org.apache.kafka.connect.data.Decimal` | decimal(p,s) | `pa.decimal128(p,s)` | If `decimal.handling.mode=precise`. Base64-encoded bytes on the wire. |
| double | — | decimal(p,s) | `pa.float64()` | If `decimal.handling.mode=double`. Same as Oracle NUMBER behavior. |
| string | — | uniqueidentifier | `pa.string()` | UUID as string. |

> **Note**: SQL Server mappings depend heavily on `decimal.handling.mode`. If set to `precise`, decimals arrive as base64-encoded byte arrays inside a struct — significantly more complex to parse than Oracle's `double` mode. Phase 2 implementation will be informed by actual sample data.

## __rowMarker__ Column

Always appended as the **last column** in every Parquet schema, regardless of source database:

```python
pa.field("__rowMarker__", pa.int32())
```

Values: 0 (insert), 1 (update), 2 (delete), 4 (upsert). See [Open Mirroring Landing Zone](open-mirroring-landing-zone.md) for mapping details.
