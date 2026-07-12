#!/usr/bin/env bash
# Build a deployable plugin zip (without Teradata JDBC).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/lib_env.sh"

"$SCRIPT_DIR/build.sh"

STAGE="$PROJECT_DIR/dist/plugin/$PLUGIN_NAME"
rm -rf "$PROJECT_DIR/dist"
mkdir -p "$STAGE"
cp "$PROJECT_DIR/trino-plugin/target/$PLUGIN_ARTIFACT" "$STAGE/"
cp "$PROJECT_DIR/trino-plugin/target/dependency/"*.jar "$STAGE/" 2>/dev/null || true

# README inside the package
cat > "$STAGE/README.txt" <<EOF
Trino Teradata Direct plugin package
====================================

1. Copy this entire directory to \$TRINO_HOME/plugin/$PLUGIN_NAME/
2. Add proprietary terajdbc4.jar into the same directory
3. Install catalog properties (see config/teradata-export.properties.example)
4. Restart Trino

Teradata JDBC is NOT included (vendor license).
EOF

VERSION=$(grep -m1 '<version>' "$PROJECT_DIR/pom.xml" | sed 's/.*<version>\(.*\)<\/version>.*/\1/')
OUT="$PROJECT_DIR/dist/trino-teradata-direct-plugin-${VERSION}.zip"
( cd "$PROJECT_DIR/dist" && zip -qr "$(basename "$OUT")" "plugin" )
echo "Created: $OUT"
echo "Contents: $(unzip -l "$OUT" | tail -1)"
