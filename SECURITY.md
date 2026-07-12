# Security Policy

## Supported versions

This project is community-maintained. Security fixes are applied on a best-effort
basis to the latest `main` / snapshot line.

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security-sensitive bugs
(especially around the binary bridge, authentication tokens, or proxy identity).

Prefer private contact with the repository maintainers (for example via GitHub
Security Advisories on the project repository).

## Hardening checklist for operators

1. Place workers and Teradata on a restricted network; do not expose bridge ports publicly.
2. Enable `teradata.export.enforce-proxy-authentication=true`.
3. Grant least-privilege `CONNECT THROUGH` on Teradata.
4. Use `teradata.password-script` or a secret manager for the service account password.
5. Keep dynamic token authentication enabled (default design).
6. Rotate service credentials if a catalog file with passwords is ever leaked.
