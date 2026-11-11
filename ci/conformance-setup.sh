#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="tests/conformance"
TARGET_FILE="$TARGET_DIR/OpenLineage.json"
EXPECTED_SHA256="69f68bee00b9beac88a87059c0102410e7bb05f3f43c46d02a0409831eceb0d2"

mkdir -p "$TARGET_DIR"

if [ -f "$TARGET_FILE" ]; then
  CURRENT_SHA256=$(sha256sum "$TARGET_FILE" | awk '{print $1}')
  if [ "$CURRENT_SHA256" = "$EXPECTED_SHA256" ]; then
    echo "OpenLineage.json already downloaded and verified."
    exit 0
  fi
fi

echo "Downloading OpenLineage 2-0-2 schema..."
curl -sSL "https://openlineage.io/spec/2-0-2/OpenLineage.json" -o "$TARGET_FILE"

ACTUAL_SHA256=$(sha256sum "$TARGET_FILE" | awk '{print $1}')
if [ "$ACTUAL_SHA256" != "$EXPECTED_SHA256" ]; then
  echo "Checksum verification failed: expected $EXPECTED_SHA256, got $ACTUAL_SHA256"
  rm -f "$TARGET_FILE"
  exit 1
fi

echo "Schema verified successfully."
