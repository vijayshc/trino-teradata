# Trino Teradata Direct Connector

**Apache License 2.0** · Unofficial community project

A high-performance Trino connector for Teradata that uses a **parallel binary data path**
(Teradata table operator → TCP bridge on Trino workers → direct Trino `Page` parsing),
as an open alternative to commercial “Direct” style connectors.

> Not affiliated with the Trino Software Foundation, Teradata Corporation, or Starburst Data.

## Features

- **Direct binary protocol** — AMP-parallel extract over TCP (not row-at-a-time JDBC fetch)
- **Embedded bridge server** on each Trino worker
- **Pushdown** — predicates, aggregations, joins, TopN/LIMIT, dynamic filtering
- **Identity propagation** — Teradata `PROXYUSER` via `QUERY_BAND`
- **Per-query security tokens** on the data plane
- **LZ4 / ZLIB compression**

## Requirements

| Component | Notes |
|-----------|--------|
| JDK 21+ | Build and runtime |
| Maven 3.8+ | Build |
| Trino 479 | Tested version (`trino.version` in root `pom.xml`) |
| Teradata | With table-operator UDF support |
| Teradata JDBC (`terajdbc4.jar`) | **Bring your own** (not redistributed) |
| Network | Teradata AMPs must reach worker bridge ports |

## Repository layout

```text
trino-teradata-direct/
├── trino-plugin/          # Trino connector (Java)
├── teradata-udf/          # ExportToTrino table operator (C + LZ4)
├── testing/trino-tests/   # Integration suite (live cluster)
├── config/                # Catalog property examples
├── scripts/               # Build, deploy, UDF register, tests
├── docs/                  # Architecture and ops guides
└── dev/                   # env.example (local.env is gitignored)
```

## Quick start

### 1. Build the plugin

```bash
./scripts/build.sh
# or: mvn -pl trino-plugin -am clean package -DskipTests
```

### 2. Deploy into Trino

```bash
export TRINO_HOME=/path/to/trino-server
export TERADATA_JDBC_JAR=/path/to/terajdbc4.jar
# optional multi-node:
# export TRINO_WORKER_1=/path/to/trino-worker-1

./scripts/deploy.sh
```

Install catalog config:

```bash
cp config/teradata-export.properties.example \
   $TRINO_HOME/etc/catalog/tdexport.properties
# edit credentials, bridge addresses, UDF database, then restart Trino
```

### 3. Register the Teradata UDF

```bash
export TD_HOST=your-td-host
export TD_LOGON_USER=dbc          # or a privileged admin
export TD_LOGON_PASSWORD=...
export TD_UDF_DATABASE=TrinoExport
# UDF sources must be readable by the Teradata node(s):
# export UDF_SRC_DIR=/path/on/td/share/teradata-udf

./scripts/register_udf.sh
RUN_BTEQ=1 ./scripts/register_udf.sh
```

Grant connect-through for identity propagation:

```sql
GRANT CONNECT THROUGH <service_user> TO PERMANENT <end_user> WITHOUT ROLE;
```

### 4. Query

```sql
SHOW SCHEMAS FROM tdexport;
SELECT COUNT(*) FROM tdexport.mydb.mytable;
```

## Lab verification (developers)

Copy and edit local overrides (never commit secrets):

```bash
cp dev/env.example dev/local.env
# or use your private lab file

./scripts/build_deploy_restart.sh
./scripts/run_tests.sh
```

Integration tests are **skipped** on plain `mvn package`. Enable with:

```bash
mvn -pl testing/trino-tests -am test -DskipITs=false
```

## Configuration reference

See `config/teradata-export.properties.example` and [docs/TECHNICAL_GUIDE_TDEXPORT.md](docs/TECHNICAL_GUIDE_TDEXPORT.md).

Key properties:

| Property | Purpose |
|----------|---------|
| `teradata.url` / `user` / `password` | JDBC control plane |
| `teradata.export.bridge-port` | Binary bridge listen port |
| `teradata.export.worker-advertised-addresses` | AMP-reachable worker list |
| `teradata.export.udf-database` | Database hosting `ExportToTrino` |
| `teradata.export.enforce-proxy-authentication` | Require PROXYUSER |

## Security notes

- The bridge opens a TCP port on workers; restrict network path from Teradata only.
- Prefer `teradata.password-script` over plaintext passwords in catalog files.
- Dynamic per-query tokens authenticate AMP connections to the bridge.
- See [SECURITY.md](SECURITY.md).

## License

Licensed under the [Apache License, Version 2.0](LICENSE).

LZ4 sources under `teradata-udf/` are BSD 2-Clause (see file headers).
Teradata JDBC is proprietary and **not** included in this repository.

## Trademark notice

Trino®, Teradata®, and Starburst® are trademarks of their respective owners.
