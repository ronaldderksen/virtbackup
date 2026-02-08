#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="virtbackup-agent.service"

if ! command -v systemctl >/dev/null 2>&1; then
  echo "systemctl not found; systemd user services are unavailable." >&2
  exit 1
fi

if [ -z "${XDG_RUNTIME_DIR:-}" ]; then
  echo "XDG_RUNTIME_DIR not set. Are you running in a user session?" >&2
  exit 1
fi

CONFIG_DIR="$HOME/.config/systemd/user"
SERVICE_PATH="$CONFIG_DIR/$SERVICE_NAME"

if systemctl --user --quiet is-enabled "$SERVICE_NAME" >/dev/null 2>&1; then
  systemctl --user disable "$SERVICE_NAME"
fi

if systemctl --user --quiet is-active "$SERVICE_NAME" >/dev/null 2>&1; then
  systemctl --user stop "$SERVICE_NAME"
fi

if [ -f "$SERVICE_PATH" ]; then
  rm -f "$SERVICE_PATH"
fi

systemctl --user daemon-reload

echo "Uninstalled user service: $SERVICE_NAME"
