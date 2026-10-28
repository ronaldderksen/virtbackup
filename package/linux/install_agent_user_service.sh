#!/usr/bin/env bash
set -euo pipefail

APP_NAME="virtbackup-agent"
SERVICE_NAME="virtbackup-agent.service"

BIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

AGENT_PATH="$BIN_DIR/$APP_NAME"
if [ ! -x "$AGENT_PATH" ]; then
  echo "Agent not found or not executable: $AGENT_PATH" >&2
  exit 1
fi

if ! command -v systemctl >/dev/null 2>&1; then
  echo "systemctl not found; systemd user services are unavailable." >&2
  exit 1
fi

SYSTEMD_VERSION_OUTPUT="$(systemctl --version)"
if [[ ! "$SYSTEMD_VERSION_OUTPUT" =~ ^systemd[[:space:]]+([0-9]+) ]] || (( BASH_REMATCH[1] < 244 )); then
  echo "systemd 244 or newer is required for installation-host restrictions." >&2
  exit 1
fi

if [ -z "${XDG_RUNTIME_DIR:-}" ]; then
  echo "XDG_RUNTIME_DIR not set. Are you running in a user session?" >&2
  exit 1
fi

CONFIG_DIR="$HOME/.config/systemd/user"
mkdir -p "$CONFIG_DIR"

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

SERVICE_PATH="$CONFIG_DIR/$SERVICE_NAME"
cat > "$SERVICE_PATH" <<SERVICE
[Unit]
Description=VirtBackup Agent (user)
ConditionHost=$INSTALL_HOST
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStartPre=$ROTATE_SCRIPT
ExecStart=$AGENT_PATH
StandardOutput=append:$LOG_PATH
StandardError=append:$LOG_PATH
Restart=on-failure
RestartSec=3

[Install]
WantedBy=default.target
SERVICE

systemctl --user daemon-reload
systemctl --user enable "$SERVICE_NAME"
systemctl --user restart "$SERVICE_NAME"

echo "Installed and started user service: $SERVICE_NAME (host: $INSTALL_HOST)"
