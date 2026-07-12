# Technical guide: Trino Teradata Direct connector

Comprehensive operator and implementer guide for the open-source Trino Teradata
Direct connector (Apache License 2.0).

| Related docs | |
|--------------|--|
| [architecture.md](architecture.md) | Control/data plane, routing, EOS overview |
| [eos.md](eos.md) | Deterministic end-of-stream details |
| [installation.md](installation.md) | Build, deploy, UDF, network |
| [configuration.md](configuration.md) | Full property reference |
| [development.md](development.md) | SPI packaging & coding standards |
| [../SECURITY.md](../SECURITY.md) | Threat model & hardening |

---

## 1. System architecture

The connector implements a **synchronous, integrity-first** pipeline: control
via JDBC, bulk data via a parallel binary bridge.

### Data flow

1. **Trino planner** — catalog `teradata_export`; pushdown via `TeradataClient` / `TeradataQueryBuilder`.
2. **SQL generation** — predicates, aggregations, joins, TopN rewritten to Teradata SQL where safe.
3. **Table operator wrap** — SQL executed as input to `ExportToTrino(...)`.
4. **AMP execution** — each participating AMP serializes rows, compresses (LZ4/ZLIB), opens one TCP socket to its assigned worker.
5. **Bridge server** — embedded in each Trino worker JVM; authenticates token, receives batches.
6. **DirectTrinoPageParser** — binary → Trino `Page` (no Arrow hot path).
7. **DataBufferRegistry** — per-query queues + **deterministic EOS**.
8. **PageSource** — feeds the Trino engine.

### AMP-to-worker distribution

```text
Teradata AMPs                               Trino workers
AMP routing_id % N == 0  ───────────────►  Worker 0 bridge
AMP routing_id % N == 1  ───────────────►  Worker 1 bridge
...
```

- Routing id: `pid ^ (pid >> 8)` on the AMP; returned to the coordinator so expectations can be recomputed with **unsigned** modulo.
- Splits are **not** remotely accessible: data locality matches bridge locality.
- Multi-worker: set `teradata.export.worker-advertised-addresses` to AMP-reachable `host:port` pairs.

### End-of-stream (summary)

**Primary:** coordinator broadcasts per-worker **expected** AMP connection counts
(`EXPECTED_TERADATA_SIGNALS`). Workers complete when received == expected.

**Not primary:** `JDBC_FINISHED` control messages are ignored (legacy). Short idle
timeouts are not the design center.

Full write-up: [eos.md](eos.md).

### Repository code map

| Path | Content |
|------|---------|
| `plugin/trino-teradata/` | Trino plugin (`packaging=trino-plugin`) |
| `teradata-udf/` | `export_to_trino.c`, LZ4 |
| `testing/trino-teradata-tests/` | Live-cluster JUnit suite |
| `config/*.example` | Catalog template |
| `scripts/` | Build, deploy, UDF register, tests |

---

## 2. Pushdown capabilities

`TeradataClient` extends Trino `BaseJdbcClient` with Teradata-specific rewrites.

| Feature | Behavior |
|---------|----------|
| Predicate pushdown | Filters, `IN`, `BETWEEN`, `LIKE`, casts, boolean trees |
| Aggregation | `COUNT`, `COUNT(DISTINCT)`, `SUM`, `MIN`, `MAX`, `AVG` |
| Join pushdown | Multi-table joins within Teradata (config toggles) |
| TopN / LIMIT | Teradata `TOP` / `SAMPLE` style limits |
| Dynamic filtering | Supported via split source + config timeout |
| System query PTF | Optional pass-through SQL table function |

Disable individual features with `teradata.export.enable-*` properties
([configuration.md](configuration.md)).

---

## 3. Metadata architecture

1. **Fast probe** — `SELECT * FROM t WHERE 1=0` for column metadata (avoids slow `DatabaseMetaData.getColumns` on large catalogs).
2. **Case probing** — exact / upper-table / upper-schema+table combinations for Teradata naming.
3. **Caches** — table handles and column definitions.
4. **MetadataRefreshService** — background warm of configured schemas via `DBC.Databases` / `DBC.TablesV` / `DBC.ColumnsV` bulk reads and targeted probes for views.

---

## 4. Security design

### 4.1 Identity

- Catalog uses a **service account** for JDBC only.
- Sessions apply `SET QUERY_BAND = 'PROXYUSER=<trino_user>;' FOR SESSION`.
- With `teradata.export.enforce-proxy-authentication=true`, proxy failure aborts the connection (no silent service-account fallback for data access).
- Connections reset with `SET QUERY_BAND = NONE FOR SESSION` before pool return.

### 4.2 Teradata grants

```sql
GRANT CONNECT THROUGH <service_user> TO PERMANENT <trino_user> WITHOUT ROLE;
```

Missing grant typically surfaces as Teradata error **9203**.

### 4.3 Data-plane tokens

Each export query uses a **dynamic security token**. AMP bridge connections must present it. Do not rely on deprecated static catalog tokens.

### 4.4 Network

Restrict worker bridge ports to the Teradata network path only. See [SECURITY.md](../SECURITY.md).

---

## 5. Implementation notes

### 5.1 C table operator (`teradata-udf/export_to_trino.c`)

- Registered as table operator `ExportToTrino` with contract function auto-discovery.
- `include/sqltypes_td.h` in-repo is a **local mock** for syntax checks; Teradata compiles against platform headers.
- Confirmed FNC type codes used by the binary codec include:

  | Type | Code |
  |------|------|
  | VARCHAR | 2 |
  | BYTEINT | 7 |
  | SMALLINT | 8 |
  | INTEGER | 9 |
  | FLOAT | 10 |
  | DECIMAL | 14 |
  | DATE | 15 |
  | TIME | 16 |
  | TIMESTAMP | 17 |
  | BIGINT | 36 |

- **TIME** — 6-byte layout: scaled seconds + hour + minute.
- **TIMESTAMP** — 10-byte layout including year/month/day/hour/minute + scaled seconds.
- **DATE** — supports pre-1900 via Teradata internal day offsets.
- **UNICODE** — UTF-16LE in Teradata → UTF-8 on the wire.
- **Routing column** — returns mixed PID so Java can compute expected per-worker counts.

### 5.2 Java plugin

| Class | Role |
|-------|------|
| `TeradataBridgeServer` | TCP accept, auth, control commands |
| `DirectTrinoPageParser` | Binary → `Page` |
| `DataBufferRegistry` | Queues + deterministic EOS |
| `TrinoExportDynamicFilteringSplitSource` | UDF execution + expected-count broadcast |
| `TrinoExportPageSource` | Engine consumer |
| `MetadataRefreshService` | Cache warming |

Legacy: `TrinoExportFlightServer` may still start for config compatibility; bulk path is the binary bridge.

---

## 6. Build, deploy, test

### 6.1 Build (Trino packaging)

```bash
export JAVA_HOME=/path/to/jdk-25
./mvnw -pl plugin/trino-teradata -am clean package
# or
./scripts/build.sh
```

Output:

```text
plugin/trino-teradata/target/trino-teradata-479-1-SNAPSHOT/
plugin/trino-teradata/target/trino-teradata-479-1-SNAPSHOT.zip
```

SPI jars are **not** bundled (`provided` scope enforced by `trino-maven-plugin`).

**Prebuilt (no Maven):** the repository also ships
`prebuilt/trino-teradata-479-1-SNAPSHOT.zip`. Unpack into
`$TRINO_HOME/plugin/teradata-export/`, copy proprietary `terajdbc4.jar` into
the same directory, configure the catalog, restart. See
[prebuilt/README.md](../prebuilt/README.md) and [installation.md](installation.md).

### 6.2 Deploy

```bash
export TRINO_HOME=/path/to/trino
export TERADATA_JDBC_JAR=/path/to/terajdbc4.jar
./scripts/deploy.sh
# multi-node lab:
# export TRINO_WORKER_1=... && ./scripts/build_deploy_restart.sh
```

Or from prebuilt:

```bash
PLUGIN_DIR=$TRINO_HOME/plugin/teradata-export
mkdir -p "$PLUGIN_DIR"
unzip -j prebuilt/trino-teradata-479-1-SNAPSHOT.zip -d "$PLUGIN_DIR"
cp "$TERADATA_JDBC_JAR" "$PLUGIN_DIR/terajdbc4.jar"
```

Catalog: copy `config/teradata-export.properties.example` →
`$TRINO_HOME/etc/catalog/tdexport.properties` and edit.

### 6.3 UDF registration

```bash
export TD_HOST=... TD_LOGON_USER=... TD_LOGON_PASSWORD=...
export UDF_SRC_DIR="$(pwd)/teradata-udf"   # must be TD-readable
./scripts/register_udf.sh
RUN_BTEQ=1 ./scripts/register_udf.sh
```

### 6.4 Integration tests

Requires a live Trino + Teradata environment:

```bash
cp dev/env.example dev/local.env   # gitignored; set lab paths
./scripts/run_tests.sh             # full suite (~230 tests)
./scripts/run_tests.sh BasicConnectivityTest
```

Maven (ITs off by default):

```bash
./mvnw -pl testing/trino-teradata-tests -am test -DskipITs=false
```

Seed data scripts (parameterize logon before use): `testing/setup_*.bteq`.

### 6.5 Log markers

```bash
grep "Executing Teradata SQL" $TRINO_HOME/data/var/log/server.log | tail
grep "DETERMINISTIC EOS" $TRINO_HOME/data/var/log/server.log | tail
```

---

## 7. Troubleshooting

| Symptom | Likely cause | Action |
|---------|--------------|--------|
| Query hangs, no rows | Expected EOS counts wrong; AMPs cannot reach bridge | Fix `worker-advertised-addresses`; check unsigned PID routing; firewall |
| Empty result but TD has data | Type schema / codec mismatch | Confirm UDF registration from current sources; check type codes |
| Access denied / 9203 | Missing `CONNECT THROUGH` | Grant to permanent user |
| Invalid stream state (7813) | Bad table-operator registration | Re-run `register_udf.sh` with void signature sources |
| Garbled strings | UNICODE without UTF-8 conversion | Ensure current UDF (UTF-16LE→UTF-8) is installed |
| TIME conversion errors | Wrong binary layout | Use 6-byte TIME decode in current parser/UDF pair |
| Plugin fails to load | Trino version ≠ 479; missing JDBC jar | Align SPI version; add `terajdbc4.jar` to plugin dir |

---

## 8. Limitations

- Optimized for **SELECT** / analytics extract, not general-purpose writes to Teradata.
- Trino version compatibility is **pinned** to the SPI version in the POM (479 today).
- Requires installing a UDF on Teradata and opening AMP→worker network paths.

---

## 9. Version history (docs)

| Date | Notes |
|------|--------|
| 2026-07 | Open-source layout: `plugin/trino-teradata`, `trino-plugin` packaging, deterministic EOS documented as primary; lab credentials removed |

*Earlier internal revisions described JDBC_FINISHED-first and lab-only paths; those sections are obsolete.*
