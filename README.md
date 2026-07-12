# Trino Teradata Direct Connector

**Apache License 2.0** · Unofficial community plugin

A high-performance Trino connector for Teradata that uses a **parallel binary data path**
(Teradata table operator → TCP bridge on Trino workers → direct Trino `Page` parsing).

> Not affiliated with the Trino Software Foundation, Teradata Corporation, or Starburst Data.

This repository is structured as a **standalone Trino plugin** following the
[SPI / `trino-plugin` packaging conventions](https://trino.io/docs/current/develop/spi-overview.html).

## Features

- **Direct binary protocol** — AMP-parallel extract over TCP
- **Embedded bridge server** on each Trino worker
- **Pushdown** — predicates, aggregations, joins, TopN/LIMIT, dynamic filtering
- **Identity propagation** — Teradata `PROXYUSER` via `QUERY_BAND`
- **Per-query security tokens** on the data plane
- **LZ4 / ZLIB compression**

## Requirements

| Component | Notes |
|-----------|--------|
| **JDK 25+** | Trino 479 SPI bytecode |
| **Maven 3.9.1+** | Use `./mvnw` (bundled) |
| Trino **479** | Pinned as `dep.trino.version` |
| Teradata | Table-operator UDF support |
| `terajdbc4.jar` | **Bring your own** (not redistributed) |

## Repository layout

Aligned with the Trino monorepo plugin layout:

```text
plugin/trino-teradata/           # packaging: trino-plugin
testing/trino-teradata-tests/    # live-cluster integration suite
teradata-udf/                    # ExportToTrino C table operator
docs/                            # architecture + development standards
config/*.example
scripts/
```

See [docs/development.md](docs/development.md) for SPI rules and coding standards.

## Build

```bash
export JAVA_HOME=/path/to/jdk-25
./mvnw clean verify                 # unit tests; ITs skipped by default
# or
./scripts/build.sh
```

Produces the official-style plugin package:

```text
plugin/trino-teradata/target/trino-teradata-479-1-SNAPSHOT/
plugin/trino-teradata/target/trino-teradata-479-1-SNAPSHOT.zip
```

## Install

```bash
export TRINO_HOME=/path/to/trino-server
export TERADATA_JDBC_JAR=/path/to/terajdbc4.jar
./scripts/deploy.sh
```

Catalog properties:

```bash
cp config/teradata-export.properties.example \
   $TRINO_HOME/etc/catalog/tdexport.properties
# edit credentials / bridge addresses, then restart Trino
```

`connector.name` must be `teradata_export`.

## Register the Teradata UDF

```bash
export TD_HOST=... TD_LOGON_USER=... TD_LOGON_PASSWORD=...
export UDF_SRC_DIR="$(pwd)/teradata-udf"
./scripts/register_udf.sh
RUN_BTEQ=1 ./scripts/register_udf.sh
```

```sql
GRANT CONNECT THROUGH <service_user> TO PERMANENT <end_user> WITHOUT ROLE;
```

## Integration tests

```bash
cp dev/env.example dev/local.env   # lab paths (gitignored)
./scripts/build_deploy_restart.sh
./scripts/run_tests.sh
```

## Documentation

| Doc | Content |
|-----|---------|
| [docs/development.md](docs/development.md) | SPI packaging, standards, build |
| [docs/TECHNICAL_GUIDE_TDEXPORT.md](docs/TECHNICAL_GUIDE_TDEXPORT.md) | Architecture & ops |
| [SECURITY.md](SECURITY.md) | Threat model & reporting |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Contribution workflow |

## License

Licensed under the [Apache License, Version 2.0](LICENSE).

LZ4 under `teradata-udf/` is BSD 2-Clause. Teradata JDBC is proprietary and not included.

## Trademark notice

Trino®, Teradata®, and Starburst® are trademarks of their respective owners.
