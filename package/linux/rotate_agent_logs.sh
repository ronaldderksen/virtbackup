#!/usr/bin/env bash
set -euo pipefail

BIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SHORT_HOST="$(hostname -s 2>/dev/null || hostname)"
LOG_PATH="$BIN_DIR/log/virtbackup-agent-${SHORT_HOST}.log"

if [ -f "$LOG_PATH.4" ]; then
  rm -f "$LOG_PATH.4"
fi
if [ -f "$LOG_PATH.3" ]; then
  mv "$LOG_PATH.3" "$LOG_PATH.4"
fi
if [ -f "$LOG_PATH.2" ]; then
  mv "$LOG_PATH.2" "$LOG_PATH.3"
fi
if [ -f "$LOG_PATH.1" ]; then
  mv "$LOG_PATH.1" "$LOG_PATH.2"
fi
if [ -f "$LOG_PATH" ]; then
  mv "$LOG_PATH" "$LOG_PATH.1"
fi
