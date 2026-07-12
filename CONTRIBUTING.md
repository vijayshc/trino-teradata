# Contributing

Thanks for improving the Trino Teradata Direct connector.

## Standards

Follow:

- https://trino.io/docs/current/develop/spi-overview.html  
- https://github.com/trinodb/trino/blob/master/.github/DEVELOPMENT.md  
- [docs/development.md](docs/development.md)  
- [docs/architecture.md](docs/architecture.md)  

### Requirements

1. Use `./mvnw` (Maven 3.9+) and **JDK 25+**
2. Keep connector packaging as `trino-plugin`
3. Never package `trino-spi` (must remain `provided`)
4. Apache-2.0 headers on Java sources (`license-header.txt`)
5. No `System.out` / `printStackTrace` — Airlift `Logger` only
6. Prefer Guava immutables and AssertJ
7. Do not commit secrets, TTU media, `terajdbc4.jar`, or lab `dev/local.env`
8. Do not change EOS to timeout-first without an architecture discussion

### Documentation

If you change behavior, update the relevant docs in the same PR:

| Change area | Update |
|-------------|--------|
| Routing / EOS | `docs/eos.md`, `docs/architecture.md` |
| Config properties | `docs/configuration.md`, `config/*.example` |
| Install / packaging | `docs/installation.md`, `README.md` |
| SPI / build | `docs/development.md` |

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
