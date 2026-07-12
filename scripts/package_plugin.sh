#!/usr/bin/env bash
# Build the trino-plugin package and publish convenience binaries under prebuilt/
# (without proprietary Teradata JDBC).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/lib_env.sh"

"$SCRIPT_DIR/build.sh"

ARTIFACT_BASE="${PLUGIN_ARTIFACT_ID}-${PROJECT_VERSION}"
TARGET_DIR="$PROJECT_DIR/$PLUGIN_MODULE/target"
SRC_ZIP="$TARGET_DIR/${ARTIFACT_BASE}.zip"
SRC_DIR="$TARGET_DIR/${ARTIFACT_BASE}"

if [[ ! -f "$SRC_ZIP" ]]; then
  echo "ERROR: expected plugin zip not found: $SRC_ZIP" >&2
  exit 1
fi

# Local staging (gitignored)
mkdir -p "$PROJECT_DIR/dist"
cp -f "$SRC_ZIP" "$PROJECT_DIR/dist/${ARTIFACT_BASE}.zip"
STAGE="$PROJECT_DIR/dist/plugin/$PLUGIN_NAME"
rm -rf "$STAGE"
mkdir -p "$STAGE"
cp "$SRC_DIR/"*.jar "$STAGE/"
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

# Published convenience binaries (committed under prebuilt/)
PREBUILT="$PROJECT_DIR/prebuilt"
mkdir -p "$PREBUILT"
cp -f "$SRC_ZIP" "$PREBUILT/${ARTIFACT_BASE}.zip"
cp -f "$TARGET_DIR/${ARTIFACT_BASE}.jar" "$PREBUILT/"
cp -f "$TARGET_DIR/${ARTIFACT_BASE}-services.jar" "$PREBUILT/"
(
  cd "$PREBUILT"
  sha256sum "${ARTIFACT_BASE}.zip" "${ARTIFACT_BASE}.jar" "${ARTIFACT_BASE}-services.jar" > SHA256SUMS.txt
)

# Safety: never ship proprietary JDBC
if unzip -l "$PREBUILT/${ARTIFACT_BASE}.zip" | grep -qiE 'terajdbc|tdgssconfig'; then
  echo "ERROR: proprietary Teradata JDBC detected in package" >&2
  exit 1
fi

echo "Created: $PROJECT_DIR/dist/${ARTIFACT_BASE}.zip"
echo "Staged:  $STAGE"
echo "Prebuilt: $PREBUILT/${ARTIFACT_BASE}.zip"
