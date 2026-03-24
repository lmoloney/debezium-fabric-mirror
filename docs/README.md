# Open Mirroring Debezium — Documentation

## Contents

- [System Architecture](architecture.md) — end-to-end data flow, components, scaling, error handling
- **Design Docs**
  - [Debezium Event Format](design/debezium-event-format.md) — envelope structure, operation codes, DDL events
  - [Open Mirroring Landing Zone](design/open-mirroring-landing-zone.md) — path layout, metadata, file conventions
  - [Type Mapping](design/type-mapping.md) — Debezium → PyArrow → Parquet type mappings
- **Decisions** — Architecture Decision Records (`decisions/`)
- **Runbooks** — Operational runbooks (`runbooks/`)
- **Source Connector Notes**
  - [Oracle](sources/oracle.md) — Oracle XE connector specifics, LogMiner, type quirks
