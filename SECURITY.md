# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it responsibly.

**Do not open a public issue.**

Instead, please email the maintainer directly or use [GitHub's private vulnerability reporting](https://github.com/lmoloney/debezium-fabric-mirror/security/advisories/new).

Include:
- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

You should receive an acknowledgment within 48 hours and a resolution timeline within 7 days.

## Scope

This policy covers the Open Mirroring Debezium pipeline code. It does **not** cover:
- Azure services (EventHub, Blob Storage, Fabric) — report those to [Microsoft Security Response Center](https://msrc.microsoft.com/)
- Debezium — report those to the [Debezium project](https://debezium.io/community/)

## Security Best Practices

When deploying this pipeline:
- **Never commit `.env` files** — use `.env.example` as a template
- **Use Managed Identity** in production instead of connection strings
- **Rotate credentials** regularly (EventHub SAS keys, Storage account keys)
- **Restrict network access** to your EventHub and Storage accounts
