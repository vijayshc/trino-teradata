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

```bash
export TRINO_HOME=/path/to/trino-server-479
export TERADATA_JDBC_JAR=/path/to/terajdbc4.jar   # BYO — not in this repo

PLUGIN_DIR="$TRINO_HOME/plugin/teradata-export"
rm -rf "$PLUGIN_DIR"
mkdir -p "$PLUGIN_DIR"

# ZIP extracts to trino-teradata-479-1-SNAPSHOT/*.jar
unzip -j prebuilt/trino-teradata-479-1-SNAPSHOT.zip -d "$PLUGIN_DIR"
cp "$TERADATA_JDBC_JAR" "$PLUGIN_DIR/terajdbc4.jar"

# Catalog (every coordinator/worker)
cp config/teradata-export.properties.example \
   "$TRINO_HOME/etc/catalog/tdexport.properties"
# edit host, credentials, worker-advertised-addresses

# Restart Trino on every node
```

Repeat the plugin install on **every** worker. Register the Teradata UDF from
`teradata-udf/` (see [docs/installation.md](../docs/installation.md)).

## Verify integrity

```bash
cd prebuilt && sha256sum -c SHA256SUMS.txt
```

## Notes

- Compatible **only** with Trino **479** (see `dep.trino.version` in the root POM).
- Do not redistribute `terajdbc4.jar` with this project.
- Prefer building from source for production hardening and supply-chain control;
  these binaries are a convenience for labs and quick evaluation.
