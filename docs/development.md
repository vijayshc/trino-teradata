# Development guide

This project follows [Trino plugin development practices](https://trino.io/docs/current/develop/spi-overview.html)
as closely as possible for a **standalone** connector (not part of the trinodb monorepo).

## Requirements

| Tool | Version | Notes |
|------|---------|--------|
| JDK | **25+** | Trino 479 SPI is Java 25 bytecode |
| Maven | **3.9.1+** | Required by `trino-maven-plugin`; use `./mvnw` |
| Trino | **479** | SPI version is pinned via `dep.trino.version` |

## Layout (mirrors Trino monorepo conventions)

```text
plugin/trino-teradata/     # connector module (packaging: trino-plugin)
testing/trino-teradata-tests/  # live-cluster integration tests
teradata-udf/              # C table operator (not a Trino plugin)
docs/
config/
scripts/
```

## SPI rules (mandatory)

From the Trino SPI overview:

1. **Plugin entry point** implements `io.trino.spi.Plugin`.
2. Packaging is **`trino-plugin`** (via `io.trino:trino-maven-plugin`), which:
   - generates the `META-INF/services` descriptor
   - assembles a plugin directory + ZIP with dependencies (Provisio)
3. **`trino-spi` is `provided`** — never ship SPI classes inside the plugin.
4. Also **`provided`**: `slice`, `jackson-annotations`, OpenTelemetry APIs
   (supplied by the Trino server classloader).
5. **Version compatibility**: build and deploy against the **same** Trino version
   (`dep.trino.version` in the root POM).

## Coding standards (aligned with Trino)

See [Trino DEVELOPMENT.md](https://github.com/trinodb/trino/blob/master/.github/DEVELOPMENT.md):

| Practice | Status in this repo |
|----------|---------------------|
| Apache-2.0 license header on sources | Enforced via `license-maven-plugin` |
| Guava `ImmutableList` / immutables | Preferred in new code |
| Airlift `Logger` (not `System.out`) | Required — no debug stdout |
| AssertJ for tests | Yes |
| No mocking libraries | Prefer hand-written fakes |
| Categorized errors (`TrinoException`) | Prefer for user-facing failures |
| `requireNonNull` on public APIs | Yes |
| Opening-brace style (Allman) | Follow in **new** code; bulk reformat deferred |

## Build

```bash
export JAVA_HOME=/path/to/jdk-25
./mvnw clean verify          # unit tests; skips live ITs
./mvnw -pl plugin/trino-teradata package
```

Plugin output (Trino-standard layout):

```text
plugin/trino-teradata/target/trino-teradata-479-1-SNAPSHOT/
plugin/trino-teradata/target/trino-teradata-479-1-SNAPSHOT.zip
```

Install by copying the directory to `$TRINO_HOME/plugin/trino-teradata/`
(or `teradata-export/`) and adding proprietary `terajdbc4.jar`.

Convenience scripts:

```bash
./scripts/build.sh
./scripts/deploy.sh                 # needs TRINO_HOME + TERADATA_JDBC_JAR
./scripts/build_deploy_restart.sh   # lab multi-node
./scripts/run_tests.sh              # full integration suite
```

## Integration tests

Live tests require Trino + Teradata. They are **skipped by default** (`skipITs=true`).

```bash
./scripts/run_tests.sh
# or
./mvnw -pl testing/trino-teradata-tests -am test -DskipITs=false
```

## License headers

All Java sources must carry the Apache-2.0 header (see `license-header.txt`).
CI greps for the header text on every PR.

## What we intentionally diverge from upstream Trino

- **groupId** is `io.github.trino-teradata` (not `io.trino`) — this is a community plugin.
- **No** full Airbase/Error Prone monorepo parent (heavy; external plugins rarely vendor it).
- Integration tests use JDBC against a real cluster (not full `trino-testing` DistributedQueryRunner) because Teradata cannot be containerized easily.
