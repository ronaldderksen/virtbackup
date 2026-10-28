#!/usr/bin/env bash
set -euo pipefail

APP_NAME="virtbackup-agent"
LABEL="com.virtbackup.agent"

BIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AGENT_PATH="$BIN_DIR/$APP_NAME"

if [ ! -x "$AGENT_PATH" ]; then
  echo "Agent not found or not executable: $AGENT_PATH" >&2
  exit 1
fi

INSTALL_HOST="$(hostname)"
if [[ ! "$INSTALL_HOST" =~ ^[a-zA-Z0-9][a-zA-Z0-9._-]*$ ]]; then
  echo "Cannot bind service to an invalid or empty hostname: $INSTALL_HOST" >&2
  exit 1
fi

LOG_DIR="$BIN_DIR/log"
mkdir -p "$LOG_DIR"
SHORT_HOST="${INSTALL_HOST%%.*}"
LOG_PATH="$LOG_DIR/virtbackup-agent-${SHORT_HOST}.log"
ROTATE_SCRIPT="$BIN_DIR/rotate_agent_logs.sh"

PLIST_DIR="$HOME/Library/LaunchAgents"
PLIST_PATH="$PLIST_DIR/${LABEL}.plist"
mkdir -p "$PLIST_DIR"

LAUNCHER_PATH="$PLIST_DIR/${LABEL}.sh"
{
  printf '#!/usr/bin/env bash\nset -euo pipefail\n\n'
  printf 'INSTALL_HOST=%q\nAGENT_PATH=%q\n\n' "$INSTALL_HOST" "$AGENT_PATH"
  cat <<'LAUNCHER'
CURRENT_HOST="$(hostname)"
if [ "$CURRENT_HOST" != "$INSTALL_HOST" ]; then
  echo "Agent not started: installed for host $INSTALL_HOST, current host is $CURRENT_HOST." >&2
  exit 0
fi
exec "$AGENT_PATH"
LAUNCHER
} > "$LAUNCHER_PATH"
chmod 700 "$LAUNCHER_PATH"

cat > "$PLIST_PATH" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>${LAUNCHER_PATH}</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key>
    <false/>
  </dict>
  <key>StandardOutPath</key>
  <string>${LOG_PATH}</string>
  <key>StandardErrorPath</key>
  <string>${LOG_PATH}</string>
</dict>
</plist>
PLIST

USER_UID="$(id -u)"
if launchctl bootout gui/${USER_UID} "$PLIST_PATH" >/dev/null 2>&1; then
  true
fi

launchctl bootstrap gui/${USER_UID} "$PLIST_PATH"
launchctl enable gui/${USER_UID}/${LABEL}
if [ -x "$ROTATE_SCRIPT" ]; then
  "$ROTATE_SCRIPT" || true
fi
launchctl kickstart -k gui/${USER_UID}/${LABEL}

echo "Installed and started launchd agent: $LABEL (host: $INSTALL_HOST)"
