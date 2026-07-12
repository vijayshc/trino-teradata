#!/usr/bin/env bash
# Copy the trino-plugin ZIP to dist/ for distribution (without Teradata JDBC).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/lib_env.sh"

"$SCRIPT_DIR/build.sh"

mkdir -p "$PROJECT_DIR/dist"
SRC_ZIP="$PROJECT_DIR/$PLUGIN_MODULE/target/${PLUGIN_ARTIFACT_ID}-${PROJECT_VERSION}.zip"
OUT="$PROJECT_DIR/dist/${PLUGIN_ARTIFACT_ID}-${PROJECT_VERSION}.zip"
cp "$SRC_ZIP" "$OUT"

# Also stage exploded plugin + README for operators
STAGE="$PROJECT_DIR/dist/plugin/$PLUGIN_NAME"
rm -rf "$STAGE"
mkdir -p "$STAGE"
cp "$PROJECT_DIR/$PLUGIN_MODULE/target/${PLUGIN_ARTIFACT_ID}-${PROJECT_VERSION}/"*.jar "$STAGE/"
cat > "$STAGE/README.txt" <<EOF
Trino Teradata Direct plugin
============================

Built with packaging=trino-plugin (trino-maven-plugin / Provisio).

1. Copy this directory to \$TRINO_HOME/plugin/$PLUGIN_NAME/
2. Add proprietary terajdbc4.jar into the same directory
3. Install catalog properties from config/teradata-export.properties.example
4. Restart Trino

Compatible Trino version: see dep.trino.version in the root pom.xml
EOF

echo "Created: $OUT"
echo "Staged:  $STAGE"
