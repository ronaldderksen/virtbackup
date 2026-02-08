#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT_DIR"

APP_NAME="virtbackup"
VERSION=$(awk -F ':' '/^version:/ {gsub(/[[:space:]]/, "", $2); print $2}' pubspec.yaml)

UNAME_S="$(uname -s)"
if [ "$UNAME_S" = "Darwin" ]; then
  "${ROOT_DIR}/tools/build_macos.sh"
else
  "${ROOT_DIR}/tools/build_linux.sh"
fi

TGZ_PATH="$ROOT_DIR/build/tgz/${APP_NAME}-${VERSION}.tgz"
TARGET_BASE="$HOME/VirtBackup"

tar -C "$TARGET_BASE" -xvf "$TGZ_PATH"

TARGET_DIR="$TARGET_BASE/${APP_NAME}-${VERSION}"
if [ -x "$TARGET_DIR/install_agent_user_service.sh" ]; then
  (cd "$TARGET_DIR" && ./install_agent_user_service.sh)
else
  echo "install_agent_user_service.sh not found in $TARGET_DIR" >&2
  exit 1
fi
