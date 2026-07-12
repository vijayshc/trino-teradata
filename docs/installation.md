# Installation

## Prerequisites

| Requirement | Notes |
|-------------|--------|
| Trino **479** | Must match `dep.trino.version` in the root POM |
| JDK **25+** (build) | SPI bytecode for Trino 479 |
| Maven **3.9.1+** | Use `./mvnw` |
| Teradata | Table operator / UDF support; AMPs can open outbound TCP to workers |
| Teradata JDBC | Proprietary `terajdbc4.jar` (not redistributed) |

## 1. Obtain the plugin

### Prebuilt (no Maven)

Convenience binaries ship under [`prebuilt/`](../prebuilt/README.md)
(Trino **479** only; **no** proprietary JDBC):

```text
prebuilt/trino-teradata-479-1-SNAPSHOT.zip   # full plugin (use this)
prebuilt/trino-teradata-479-1-SNAPSHOT.jar   # connector classes only
prebuilt/SHA256SUMS.txt
```

```bash
cd prebuilt && sha256sum -c SHA256SUMS.txt
```

Refresh published binaries after a source change:

```bash
./scripts/package_plugin.sh
```

### Build from source

```bash
export JAVA_HOME=/path/to/jdk-25
./mvnw -pl plugin/trino-teradata -am clean package
# or
./scripts/build.sh
```

Artifacts (Trino `trino-plugin` packaging / Provisio):

```text
plugin/trino-teradata/target/trino-teradata-479-1-SNAPSHOT/
plugin/trino-teradata/target/trino-teradata-479-1-SNAPSHOT.zip
```

## 2. Install into Trino

```bash
export TRINO_HOME=/path/to/trino-server
export TERADATA_JDBC_JAR=/path/to/terajdbc4.jar
# multi-node lab optional:
# export TRINO_WORKER_1=/path/to/worker-root

./scripts/deploy.sh
```

Or manually from **prebuilt** or **target**:

```bash
PLUGIN_DIR=$TRINO_HOME/plugin/teradata-export   # directory name is free-form
mkdir -p "$PLUGIN_DIR"
# prebuilt:
unzip -j prebuilt/trino-teradata-479-1-SNAPSHOT.zip -d "$PLUGIN_DIR"
# or from a local Maven build:
# cp plugin/trino-teradata/target/trino-teradata-479-1-SNAPSHOT/*.jar "$PLUGIN_DIR/"
cp "$TERADATA_JDBC_JAR" "$PLUGIN_DIR/terajdbc4.jar"
```

Repeat on **every** worker. Restart Trino after install.

## 3. Catalog configuration

```bash
cp config/teradata-export.properties.example \
   $TRINO_HOME/etc/catalog/tdexport.properties
```

Edit at least:

```properties
connector.name=teradata_export
teradata.url=jdbc:teradata://YOUR_TD_HOST/DATABASE=DBC,TMODE=TERA
teradata.user=YOUR_SERVICE_USER
teradata.password=YOUR_PASSWORD
# or: teradata.password-script=/etc/trino/secrets/td-password.sh

teradata.export.bridge-port=9999
teradata.export.worker-advertised-addresses=WORKER1_IP:9999,WORKER2_IP:9999
teradata.export.udf-database=TrinoExport
teradata.export.enforce-proxy-authentication=true
```

**Important:** `connector.name` must be `teradata_export` (underscore), matching
`TrinoExportConnectorFactory.getName()`.

Full property list: [configuration.md](configuration.md).

## 4. Register the Teradata UDF

Sources under `teradata-udf/` must be readable by Teradata nodes (shared filesystem
or copy onto the node before `REPLACE FUNCTION`).

```bash
export TD_HOST=your-td-host
export TD_LOGON_USER=admin_user
export TD_LOGON_PASSWORD=...
export TD_UDF_DATABASE=TrinoExport
export UDF_SRC_DIR=/path/visible/to/teradata/teradata-udf

./scripts/register_udf.sh          # generates scripts/register.generated.bteq
RUN_BTEQ=1 ./scripts/register_udf.sh
```

Grant identity propagation for each interactive Trino user:

```sql
GRANT CONNECT THROUGH <service_user> TO PERMANENT <trino_user> WITHOUT ROLE;
```

## 5. Network

- Teradata AMPs → each advertised worker bridge port (TCP).
- Do **not** expose bridge ports to untrusted networks.
- Trino coordinator → Teradata JDBC port (typically 1025).

## 6. Smoke test

```sql
SHOW SCHEMAS FROM tdexport;
SELECT COUNT(*) FROM tdexport.<schema>.<table> LIMIT 1;
```

Check server logs for bridge accept lines and `DETERMINISTIC EOS` messages.

## Uninstall

Remove `$TRINO_HOME/plugin/teradata-export` (or your plugin directory name),
remove the catalog properties file, and restart Trino. Drop the UDF on Teradata
if no longer needed:

```sql
DATABASE TrinoExport;
DROP FUNCTION ExportToTrino;
DROP FUNCTION ExportToTrino_contract;
```
