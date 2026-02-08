#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT_DIR"

APP_NAME="virtbackup"
VERSION=$(awk -F ':' '/^version:/ {gsub(/[[:space:]]/, "", $2); print $2}' pubspec.yaml)

build_agent() {
  local out_path="$1"
  local out_dir
  if dart build --help >/dev/null 2>&1; then
    out_dir="$(dirname "$out_path")/agent-build"
    rm -rf "$out_dir"
    mkdir -p "$out_dir"
    dart build cli --target "$ROOT_DIR/bin/agent.dart" -o "$out_dir"
    if [ -f "$out_dir/bundle/bin/virtbackup-agent" ]; then
      cp "$out_dir/bundle/bin/virtbackup-agent" "$out_path"
    else
      cp "$out_dir/bundle/bin/agent" "$out_path"
    fi
  else
    dart compile exe "$ROOT_DIR/bin/agent.dart" -o "$out_path"
  fi
}

"${FLUTTER:-flutter}" build macos --release

BUNDLE_DIR="$ROOT_DIR/build/macos/Build/Products/Release"
OUT_DIR="$ROOT_DIR/build/tgz"
STAGE_DIR="$OUT_DIR/${APP_NAME}-${VERSION}"
AGENT_OUT="$ROOT_DIR/build/macos/Build/Products/Release/virtbackup-agent"

rm -rf "$STAGE_DIR"
mkdir -p "$STAGE_DIR"

cp -r "$BUNDLE_DIR/"* "$STAGE_DIR/"

build_agent "$AGENT_OUT"
cp "$AGENT_OUT" "$STAGE_DIR/virtbackup-agent"
if [ -d "$ROOT_DIR/package/macos" ]; then
  cp -r "$ROOT_DIR/package/macos/." "$STAGE_DIR/"
fi

if [ -x "$ROOT_DIR/hashblocks/hashblocks" ]; then
  cp "$ROOT_DIR/hashblocks/hashblocks" "$STAGE_DIR/hashblocks"
else
  echo "hashblocks binary not found at $ROOT_DIR/hashblocks/hashblocks" >&2
  exit 1
fi

make -C "$ROOT_DIR/native" clean
make -C "$ROOT_DIR/native"
NATIVE_DIR="$STAGE_DIR/native/macos"
mkdir -p "$NATIVE_DIR"
if [ -f "$ROOT_DIR/native/macos/libvirtbackup_native.dylib" ]; then
  cp "$ROOT_DIR/native/macos/libvirtbackup_native.dylib" "$NATIVE_DIR/libvirtbackup_native.dylib"
else
  echo "native lib not found at $ROOT_DIR/native/macos/libvirtbackup_native.dylib (skipping)" >&2
fi

mkdir -p "$OUT_DIR"
TAR_PATH="$OUT_DIR/${APP_NAME}-${VERSION}.tgz"

tar -C "$OUT_DIR" -czf "$TAR_PATH" "${APP_NAME}-${VERSION}"

echo "TGZ created: $TAR_PATH"
