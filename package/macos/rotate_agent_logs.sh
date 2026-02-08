#!/usr/bin/env bash
set -euo pipefail

LOG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/log"
APP_NAME="virtbackup-agent"
SHORT_HOST="$(hostname -s 2>/dev/null || hostname)"
LOG_PATH="$LOG_DIR/${APP_NAME}-${SHORT_HOST}.log"
MAX_SIZE_MB=10
MAX_BACKUPS=5

mkdir -p "$LOG_DIR"

if [ ! -f "$LOG_PATH" ]; then
  exit 0
fi

# Check size in MB
SIZE_MB=$(du -m "$LOG_PATH" | awk '{print $1}')
if [ "$SIZE_MB" -lt "$MAX_SIZE_MB" ]; then
  exit 0
fi

# Rotate logs: .1 is newest
for ((i=MAX_BACKUPS-1; i>=1; i--)); do
  if [ -f "$LOG_PATH.$i" ]; then
    mv "$LOG_PATH.$i" "$LOG_PATH.$((i+1))"
  fi
done

mv "$LOG_PATH" "$LOG_PATH.1"
: > "$LOG_PATH"
