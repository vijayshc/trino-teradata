# Development guide

This project follows [Trino plugin development practices](https://trino.io/docs/current/develop/spi-overview.html)
as closely as practical for a **standalone** community connector (not part of the trinodb monorepo).

## Requirements

| Tool | Version | Notes |
|------|---------|--------|
| JDK | **25+** | Trino 479 SPI is Java 25 bytecode |
| Maven | **3.9.1+** | Required by `trino-maven-plugin`; use `./mvnw` |
| Trino | **479** | SPI version pinned via `dep.trino.version` |

## Layout

```text
plugin/trino-teradata/          # packaging: trino-plugin
testing/trino-teradata-tests/   # live-cluster integration tests
testing/trino-teradata-stress/  # concurrent stress + bottleneck CLI
teradata-udf/                   # C table operator (not a Trino plugin)
docs/                           # architecture, install, EOS, config
config/                         # *.example only (no secrets)
scripts/
```

## SPI rules (mandatory)

1. Plugin entry implements `io.trino.spi.Plugin` (`TrinoExportPlugin`).
2. Module packaging is **`trino-plugin`** (`io.trino:trino-maven-plugin`):
   - generates the `META-INF/services` descriptor
   - Provisio assembles `target/<artifact>-<version>/` + `.zip`
3. **`trino-spi` is `provided`** — never ship SPI classes in the plugin.
4. Also **`provided`**: `slice`, `jackson-annotations`, OpenTelemetry APIs
   (and incubator/common as required by the SPI checker).
5. Deploy with the **same** Trino version used to compile
   ([compatibility](https://trino.io/docs/current/develop/spi-overview.html#compatibility)).

## Coding standards

Aligned with [Trino DEVELOPMENT.md](https://github.com/trinodb/trino/blob/master/.github/DEVELOPMENT.md):

| Practice | Status |
|----------|--------|
| Apache-2.0 license header on Java sources | Required; CI greps for header text |
| Guava `ImmutableList` / immutables | Preferred in new code |
| Airlift `Logger` (not `System.out`) | Required |
| AssertJ for tests | Yes |
| No mocking libraries | Prefer hand-written fakes |
| `TrinoException` for user-facing errors | Prefer categorized errors |
| `requireNonNull` on public APIs | Yes |
| Allman brace style | Prefer for **new** code; bulk reformat deferred |
| Deterministic EOS (not timeout-first) | Do not regress without design review |

## Architecture to preserve

- **Control plane:** JDBC + pushdown + UDF invocation  
- **Data plane:** binary bridge + `DirectTrinoPageParser`  
- **EOS:** expected per-worker AMP counts from routing PIDs ([eos.md](eos.md))  
- **Security:** `PROXYUSER` + per-query tokens  

Do not reintroduce JDBC_FINISHED-driven EOS as the primary path.

## Build

```bash
export JAVA_HOME=/path/to/jdk-25
./mvnw clean verify
./mvnw -pl plugin/trino-teradata package
./mvnw -pl testing/trino-teradata-stress -am package -DskipTests
./scripts/run_stress.sh quick    # live cluster required
```

See [testing/trino-teradata-stress/README.md](../testing/trino-teradata-stress/README.md).

Plugin output:

```text
plugin/trino-teradata/target/trino-teradata-479-1-SNAPSHOT/
plugin/trino-teradata/target/trino-teradata-479-1-SNAPSHOT.zip
```

Install directory name under `$TRINO_HOME/plugin/` is free-form (scripts use
`teradata-export`); always add proprietary `terajdbc4.jar`.

```bash
./scripts/build.sh
./scripts/deploy.sh                 # TRINO_HOME + TERADATA_JDBC_JAR
./scripts/build_deploy_restart.sh   # multi-node lab helper
./scripts/run_tests.sh              # integration suite
```

## Integration tests

Live tests need Trino + Teradata. Default Maven build sets `skipITs=true`.

```bash
./scripts/run_tests.sh
./mvnw -pl testing/trino-teradata-tests -am test -DskipITs=false
```

Env overrides: `TRINO_JDBC_URL`, `TRINO_USER`, `TRINO_SERVER_LOG` (see `dev/env.example`).

## License headers

All Java sources must include the Apache-2.0 header (`license-header.txt`).
CI fails if the license line is missing.

## Intentional divergences from upstream Trino

| Topic | Upstream | This repo |
|-------|----------|-----------|
| groupId | `io.trino` | `io.github.trino-teradata` (community) |
| Parent POM | Airbase monorepo | Lightweight standalone parent |
| Integration tests | `trino-testing` / containers | Live JDBC suite (Teradata not easily containerized) |
| Brace style | Strict Allman | Preferred for new code only |

## Useful links

- [architecture.md](architecture.md)
- [installation.md](installation.md)
- [configuration.md](configuration.md)
- [TECHNICAL_GUIDE_TDEXPORT.md](TECHNICAL_GUIDE_TDEXPORT.md)
