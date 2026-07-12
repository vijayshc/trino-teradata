# Contributing

Thanks for your interest in improving the Trino Teradata Direct connector.

## Development setup

1. JDK 21+, Maven 3.8+
2. Optional: local Trino 479 cluster and a Teradata instance for integration tests
3. Copy `dev/env.example` → `dev/local.env` for lab paths (gitignored)

```bash
./scripts/build.sh
```

## Code guidelines

- Prefer small, focused PRs
- Do not commit secrets, lab IPs with credentials, TTU media, or Teradata JDBC JARs
- Keep the data path deterministic (AMP routing + expected EOS); do not switch to timeout-first EOS without design discussion
- Integration tests belong under `testing/trino-tests` and must honor env/system properties

## Tests

```bash
# Unit / compile only (default)
mvn -pl trino-plugin -am test

# Full integration suite (live cluster required)
./scripts/run_tests.sh
```

## License

By contributing, you agree that your contributions are licensed under the
Apache License 2.0.
