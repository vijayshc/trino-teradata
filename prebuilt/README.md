# Prebuilt plugin artifacts

Published binaries for **Trino 479**, built with `packaging=trino-plugin`
(Provisio). These do **not** include proprietary Teradata JDBC.

| File | Description |
|------|-------------|
| `trino-teradata-479-1-SNAPSHOT.zip` | **Install this** — full plugin directory (connector + runtime deps) |
| `trino-teradata-479-1-SNAPSHOT.jar` | Connector classes only (not sufficient alone) |
| `trino-teradata-479-1-SNAPSHOT-services.jar` | SPI services descriptor |
| `SHA256SUMS.txt` | Checksums for the files above |

Rebuild / refresh:

```bash
./scripts/package_plugin.sh   # also updates prebuilt/
```

## Install (no Maven required)

Typical flow: **unpack prebuilt ZIP → add `terajdbc4.jar` → catalog → restart**.

```bash
export TRINO_HOME=/path/to/trino-server-479
export TERADATA_JDBC_JAR=/path/to/terajdbc4.jar   # BYO — not in this repo

PLUGIN_DIR="$TRINO_HOME/plugin/teradata-export"
rm -rf "$PLUGIN_DIR"
mkdir -p "$PLUGIN_DIR"

# 1) Open-source connector + runtime dependency JARs
#    ZIP contains trino-teradata-479-1-SNAPSHOT/*.jar; -j flattens into PLUGIN_DIR
unzip -j prebuilt/trino-teradata-479-1-SNAPSHOT.zip -d "$PLUGIN_DIR"

# 2) Proprietary Teradata JDBC (required for the plugin to load / connect)
cp "$TERADATA_JDBC_JAR" "$PLUGIN_DIR/terajdbc4.jar"

# 3) Catalog on the coordinator
cp config/teradata-export.properties.example \
   "$TRINO_HOME/etc/catalog/tdexport.properties"
# edit: teradata.url, user/password, worker-advertised-addresses
# connector.name must remain teradata_export

# 4) Restart Trino on every node that received the plugin
```

Repeat steps 1–2 (plugin + JDBC) on **every** worker, then restart those nodes.
Register the Teradata UDF from `teradata-udf/` (see
[docs/installation.md](../docs/installation.md)).

After restart, a basic check:

```sql
SHOW CATALOGS;
SHOW SCHEMAS FROM tdexport;
```

## Verify integrity

```bash
cd prebuilt && sha256sum -c SHA256SUMS.txt
```

## Notes

- Compatible **only** with Trino **479** (see `dep.trino.version` in the root POM).
- Do not redistribute `terajdbc4.jar` with this project.
- Prefer building from source for production hardening and supply-chain control;
  these binaries are a convenience for labs and quick evaluation.
