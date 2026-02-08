#!/usr/bin/env bash
set -euo pipefail

LABEL="com.virtbackup.agent"
PLIST_PATH="$HOME/Library/LaunchAgents/${LABEL}.plist"
USER_ID="$(id -u)"

if [ -f "$PLIST_PATH" ]; then
  launchctl bootout gui/${USER_ID} "$PLIST_PATH" >/dev/null 2>&1 || true
  rm -f "$PLIST_PATH"
  launchctl bootout gui/${USER_ID}/${LABEL} >/dev/null 2>&1 || true
fi

launchctl disable gui/${USER_ID}/${LABEL} >/dev/null 2>&1 || true

echo "Uninstalled launchd agent: $LABEL"
