# Security Policy

## Supported versions

This project is community-maintained. Security fixes are applied on a best-effort
basis to the latest release line matching a supported Trino SPI version
(see `dep.trino.version` in the root POM).

## Reporting a vulnerability

**Do not** open a public GitHub issue for security-sensitive bugs involving:

- binary bridge authentication or tokens
- proxy / `PROXYUSER` identity bypass
- credential handling (`password-script`, catalog secrets)
- remote code paths on the data plane

Prefer GitHub **Security Advisories** on the project repository, or private
contact with the maintainers listed in the repository metadata.

## Threat model (summary)

| Surface | Risk | Mitigation |
|---------|------|------------|
| Worker bridge TCP port | Untrusted clients could attempt to inject data | Per-query tokens; network allowlist Teradata→workers only |
| Service account JDBC | Over-privileged service user | Least privilege; `enforce-proxy-authentication=true` |
| Catalog properties | Password leakage in VCS or images | `teradata.password-script` / secret manager; never commit real configs |
| UDF source on TD nodes | Tampered table operator | Control filesystem access; review registration scripts |

## Operator hardening checklist

1. Do **not** expose bridge ports publicly or on shared flat networks.
2. Keep `teradata.export.enforce-proxy-authentication=true`.
3. Grant least-privilege `CONNECT THROUGH ... TO PERMANENT ...` only for real users.
4. Prefer `teradata.password-script` over plaintext `teradata.password`.
5. Rely on dynamic per-query tokens (do not reintroduce static shared secrets).
6. Rotate service credentials if a catalog file with passwords is ever leaked.
7. Run Trino and the plugin on a version pair you have tested together.
8. Monitor logs for unexpected bridge accepts and EOS fallback timeouts.

## Architectural notes

Identity and data-plane security are described in
[docs/architecture.md](docs/architecture.md) and [docs/eos.md](docs/eos.md).
