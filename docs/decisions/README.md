# Decision Records

Architectural decisions for the Debezium → Fabric Open Mirroring pipeline.

Format: lightweight ADR with YAML frontmatter (`id`, `title`, `status`, `date`, `tags`).

| ID | Title | Status | Date |
|----|-------|--------|------|
| [001](001-container-apps-over-functions.md) | Container Apps over Azure Functions | accepted | 2026-03-24 |
| [002](002-upsert-over-update.md) | Upsert over Update for Debezium change events | accepted | 2026-03-24 |
| [003](003-last-update-time-file-detection.md) | LastUpdateTimeFileDetection over sequential numbering | accepted | 2026-03-24 |
| [004](004-auto-discover-tables-from-ddl.md) | Auto-discover tables from Debezium DDL events | accepted | 2026-03-24 |
| [005](005-default-azure-credential.md) | DefaultAzureCredential for all authentication | accepted | 2026-03-24 |
| [006](006-drop-rename-for-direct-upload.md) | Drop temp-file-then-rename for direct upload | accepted | 2026-03-24 |
| [007](007-event-buffer-flush-thresholds.md) | Per-table event buffering with configurable flush thresholds | accepted | 2026-03-24 |
| [008](008-flush-on-empty-batches.md) | Flush buffered events on empty EventHub batches | accepted | 2026-03-25 |
