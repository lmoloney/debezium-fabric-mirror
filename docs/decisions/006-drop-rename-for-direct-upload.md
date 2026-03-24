---
id: "006"
title: "Drop temp-file-then-rename for direct upload to OneLake"
status: accepted
date: 2026-03-24
tags: [onelake, upload, reliability]
---

## Context

The original `upload_parquet` implementation used a temp-file-then-rename pattern for
atomic writes: upload to `_<uuid>.parquet`, then rename to `<uuid>.parquet`. This
consistently failed with `SourcePathNotFound` because OneLake's DFS endpoint does not
support the Python SDK's `rename_file()` operation.

Microsoft's own Open Mirroring Python SDK (`fabric-toolbox/OpenMirroringPythonSDK`)
confirms this limitation with the comment: "Python SDK doesn't handle rename properly
for onelake, using REST API to rename the file instead." Their workaround uses a raw
REST API call with the `x-ms-rename-source` header.

## Decision

Upload Parquet files directly to their final UUID filename using `upload_data(overwrite=True)`,
without the temp-file-then-rename step.

## Alternatives considered

1. **REST API rename** (like the official SDK): Would preserve atomicity but adds a
   separate HTTP call and manual token management. The benefit is minimal given our
   file detection strategy.
2. **Keep temp-file-then-rename**: Not viable — OneLake doesn't support it via the
   Python SDK.

## Consequences

- **Positive**: Uploads work reliably. Simpler code path. No dependency on rename support.
- **Acceptable risk**: A crash mid-upload could leave a partial Parquet file. This is
  mitigated by: (a) UUID filenames mean no collision, (b) Parquet is self-describing so
  a partial file would fail to parse rather than produce corrupt data, (c) our
  `fileDetectionStrategy: LastUpdateTimeFileDetection` means mirroring reads by timestamp
  and would simply skip or retry a malformed file.
