# Contributing

Thanks for improving the Trino Teradata Direct connector.

## Standards

This project follows Trino plugin conventions described in:

- https://trino.io/docs/current/develop/spi-overview.html
- https://github.com/trinodb/trino/blob/master/.github/DEVELOPMENT.md
- [docs/development.md](docs/development.md) (project-specific)

Key requirements:

1. Use `./mvnw` (Maven 3.9+) and **JDK 25+**
2. Keep `packaging` as `trino-plugin` for the connector module
3. Never package `trino-spi` (must stay `provided`)
4. Apache-2.0 license headers on Java sources (template: `license-header.txt`)
5. No `System.out` / `printStackTrace` — use Airlift `Logger`
6. Prefer Guava immutables and AssertJ
7. Do not commit secrets, TTU media, or `terajdbc4.jar`

## Workflow

```bash
export JAVA_HOME=/path/to/jdk-25
./mvnw -pl plugin/trino-teradata -am verify
```

Integration tests (live cluster):

```bash
./scripts/run_tests.sh
```

By contributing, you agree that your contributions are licensed under Apache-2.0.
