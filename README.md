# Trino Teradata Direct Connector

**Apache License 2.0** · Unofficial community plugin

High-performance **read** connector for [Trino](https://trino.io) that extracts data from
Teradata using a **parallel binary path** (table operator on AMPs → TCP bridge on Trino
workers → direct `Page` parsing), as an open alternative to commercial Direct-style
connectors.

> Not affiliated with the Trino Software Foundation or Teradata Corporation.

Packaging follows Trino’s
[`trino-plugin` SPI conventions](https://trino.io/docs/current/develop/spi-overview.html).

## Why this exists

Generic JDBC extract is row-at-a-time and coordinator-heavy. This connector:

1. Pushes computation to Teradata (predicates, aggs, joins, TopN).
2. Streams result rows **in parallel from AMPs** to Trino workers.
3. Completes splits with **deterministic end-of-stream** (expected AMP counts per worker),
   not timeout-first signaling.

## Features

- Direct binary protocol (LZ4 / ZLIB compressed batches)
- Embedded bridge server on every Trino worker
- Predicate, aggregation, join, TopN, and dynamic-filter pushdown
- Teradata `PROXYUSER` identity propagation
- Per-query data-plane security tokens
- Background metadata cache refresh

## Requirements

| Component | Notes |
|-----------|--------|
| **JDK 25+** | Build against Trino 479 SPI |
| **Maven 3.9.1+** | Bundled as `./mvnw` |
| **Trino 479** | Pinned via `dep.trino.version` |
| Teradata | Table-operator UDF support; AMP→worker TCP |
| `terajdbc4.jar` | **Bring your own** (not in this repo) |

## Repository layout

```text
plugin/trino-teradata/            # Trino plugin (packaging: trino-plugin)
teradata-udf/                     # ExportToTrino C table operator + LZ4
testing/trino-teradata-tests/     # Live-cluster integration tests
config/*.example                  # Catalog template (no secrets)
scripts/                          # build / deploy / UDF / tests
docs/                             # architecture, install, EOS, config
```

## Quick start

```bash
# Build
export JAVA_HOME=/path/to/jdk-25
./mvnw -pl plugin/trino-teradata -am clean package

# Deploy (every Trino node)
export TRINO_HOME=/path/to/trino-server
export TERADATA_JDBC_JAR=/path/to/terajdbc4.jar
./scripts/deploy.sh

# Catalog
cp config/teradata-export.properties.example \
   $TRINO_HOME/etc/catalog/tdexport.properties
# edit host, credentials, worker-advertised-addresses → restart Trino

# UDF on Teradata
export TD_HOST=... TD_LOGON_USER=... TD_LOGON_PASSWORD=...
export UDF_SRC_DIR="$(pwd)/teradata-udf"   # path must be readable by TD
./scripts/register_udf.sh && RUN_BTEQ=1 ./scripts/register_udf.sh
```

```sql
GRANT CONNECT THROUGH <service_user> TO PERMANENT <trino_user> WITHOUT ROLE;

SHOW SCHEMAS FROM tdexport;
SELECT COUNT(*) FROM tdexport.<schema>.<table>;
```

`connector.name` must be **`teradata_export`**.

## Documentation

| Document | Contents |
|----------|----------|
| [docs/architecture.md](docs/architecture.md) | Control/data plane, routing, security model |
| [docs/eos.md](docs/eos.md) | Deterministic EOS design |
| [docs/installation.md](docs/installation.md) | Build, install, UDF, network |
| [docs/configuration.md](docs/configuration.md) | Catalog property reference |
| [docs/TECHNICAL_GUIDE_TDEXPORT.md](docs/TECHNICAL_GUIDE_TDEXPORT.md) | Full technical guide |
| [docs/development.md](docs/development.md) | SPI packaging & contributor standards |
| [SECURITY.md](SECURITY.md) | Hardening & vulnerability reporting |
| [CONTRIBUTING.md](CONTRIBUTING.md) | How to contribute |

## Development & tests

```bash
./mvnw clean verify                          # unit tests; ITs skipped
cp dev/env.example dev/local.env             # gitignored lab overrides
./scripts/build_deploy_restart.sh            # optional multi-node lab
./scripts/run_tests.sh                       # full integration suite
```

See [docs/development.md](docs/development.md).

## Compatibility

Full compatibility is only guaranteed when the plugin is built for the **same**
Trino version it is deployed to (`dep.trino.version` in the root `pom.xml`).
See the [Trino SPI compatibility notes](https://trino.io/docs/current/develop/spi-overview.html#compatibility).

## License

Licensed under the [Apache License, Version 2.0](LICENSE).

- LZ4 sources under `teradata-udf/` are **BSD 2-Clause**
- Teradata JDBC is proprietary and **must not** be redistributed with this project

## Trademarks

Trino®, Teradata®, and Starburst® are trademarks of their respective owners.
