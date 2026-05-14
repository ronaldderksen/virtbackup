#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
cd "$ROOT_DIR"

RELEASE=false
for arg in "$@"; do
  case "$arg" in
    --release)
      RELEASE=true
      ;;
    *)
      echo "Unknown argument: $arg" >&2
      echo "Usage: tools/local_build.sh [--release]" >&2
      exit 1
      ;;
  esac
done

APP_NAME="virtbackup"
VERSION=$(awk -F ':' '/^version:/ {gsub(/[[:space:]]/, "", $2); split($2, parts, "+"); print parts[1]}' pubspec.yaml)

UNAME_S="$(uname -s)"
if [ "$UNAME_S" = "Darwin" ]; then
  PLATFORM_NAME="macos"
  UNAME_M="$(uname -m)"
  case "$UNAME_M" in
    arm64)
      ARCH_NAME="arm64"
      ;;
    x86_64)
      ARCH_NAME="x64"
      ;;
    *)
      echo "Unsupported macOS architecture: $UNAME_M" >&2
      exit 1
      ;;
  esac
  "${ROOT_DIR}/tools/build_macos.sh"
else
  PLATFORM_NAME="linux"
  ARCH_NAME="x64"
  "${ROOT_DIR}/tools/build_linux.sh"
fi

TGZ_PATH="$ROOT_DIR/build/tgz/${APP_NAME}-${PLATFORM_NAME}-${ARCH_NAME}-${VERSION}.tgz"
TARGET_BASE="$HOME/VirtBackup"

tar -C "$TARGET_BASE" -xvf "$TGZ_PATH"

TARGET_DIR="$TARGET_BASE/${APP_NAME}-${VERSION}"
if [ -x "$TARGET_DIR/install_agent_user_service.sh" ]; then
  (cd "$TARGET_DIR" && ./install_agent_user_service.sh)
else
  echo "install_agent_user_service.sh not found in $TARGET_DIR" >&2
  exit 1
fi

if [ "$RELEASE" = true ]; then
  BACKEND_PUBLIC_DIR="$SCRIPT_DIR/../../virtbackup_backend/public"
  RELEASE_DIR="$BACKEND_PUBLIC_DIR/downloads"
  CHANGELOG_RELEASE_PATH="$BACKEND_PUBLIC_DIR/CHANGELOG.md"
  if [ ! -d "$RELEASE_DIR" ]; then
    echo "Release directory not found: $RELEASE_DIR" >&2
    exit 1
  fi
  RELEASE_PATH="$RELEASE_DIR/$(basename "$TGZ_PATH")"
  if [ -e "$RELEASE_PATH" ]; then
    echo "Release already exists: $RELEASE_PATH" >&2
    echo "Refusing to overwrite existing release." >&2
    exit 1
  fi
  cp "$TGZ_PATH" "$RELEASE_PATH"
  cp "$ROOT_DIR/CHANGELOG.md" "$CHANGELOG_RELEASE_PATH"
  echo "Released TGZ: $RELEASE_PATH"
  echo "Released changelog: $CHANGELOG_RELEASE_PATH"
fi
