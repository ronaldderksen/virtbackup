#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT_DIR"

UNAME_S="$(uname -s)"
if [ "$UNAME_S" = "Darwin" ]; then
  "${ROOT_DIR}/tools/build_macos.sh"
else
  "${ROOT_DIR}/tools/build_linux.sh"
fi
