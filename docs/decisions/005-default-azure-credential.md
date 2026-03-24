---
id: "005"
title: "DefaultAzureCredential for all authentication"
status: accepted
date: 2026-03-24
tags: [auth, infrastructure]
---

## Context

The pipeline needs to authenticate to both OneLake (ADLS Gen2 API) and optionally EventHub. Need to support local development (where a service principal is typical) and production (where managed identity is preferred). The Microsoft OpenMirroringPythonSDK uses `ClientSecretCredential` directly, which hard-codes one credential type.

## Decision

Use `DefaultAzureCredential` from `azure-identity` everywhere. Single auth code path for all environments.

## Rationale

`DefaultAzureCredential` automatically chains credential providers: environment variables (SPN) → managed identity → Azure CLI → etc. No code changes between local dev and prod.

Alternatives considered:

- **`ClientSecretCredential` directly** (what the SDK does) — works, but requires secrets in every environment including prod. No managed identity support without code changes.
- **Environment-specific credential selection** — unnecessary complexity; `DefaultAzureCredential` already does this.

## Consequences

- In prod, the Container App's managed identity needs Contributor role on the Fabric workspace.
- For local dev, set `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` env vars (or just use `az login`).
- No secrets in code or config files.
- Slightly slower first-token acquisition (chains through providers), negligible in practice since tokens are cached.
