#!/usr/bin/env bash
set -euo pipefail

usage() {
  printf 'Usage: %s <disk-device> <repeat-count>\n' "$(basename "$0")" >&2
  printf 'Example: %s /dev/rdisk4 50\n' "$(basename "$0")" >&2
}

fail() {
  printf 'ERROR: %s\n' "$1" >&2
  exit 1
}

if [ "$#" -ne 2 ]; then
  usage
  exit 2
fi

DEVICE="$1"
REPEAT_COUNT="$2"
BLOCK_SIZE=$((4 * 1024 * 1024))
TMP_BLOCK="$(mktemp "${TMPDIR:-/tmp}/virtbackup-random-block.XXXXXX")"

cleanup() {
  rm -f "$TMP_BLOCK"
}
trap cleanup EXIT

case "$DEVICE" in
  /dev/*) ;;
  *) fail "disk device must be an absolute /dev path" ;;
esac

if [ ! -e "$DEVICE" ]; then
  fail "disk device does not exist: $DEVICE"
fi

if [ ! -b "$DEVICE" ] && [ ! -c "$DEVICE" ]; then
  fail "path is not a block or character device: $DEVICE"
fi

case "$REPEAT_COUNT" in
  ''|*[!0-9]*) fail "repeat-count must be a positive integer" ;;
esac

if [ "$REPEAT_COUNT" -le 0 ]; then
  fail "repeat-count must be greater than zero"
fi

check_mounted_linux() {
  if findmnt --source "$DEVICE" --noheadings --output TARGET 2>/dev/null | grep -q .; then
    fail "device is mounted according to findmnt: $DEVICE"
  fi
  if lsblk --noheadings --raw --output MOUNTPOINT "$DEVICE" 2>/dev/null | awk 'NF { found=1 } END { exit found ? 0 : 1 }'; then
    fail "device or one of its children is mounted according to lsblk: $DEVICE"
  fi
}

check_mounted_macos() {
  local info
  info="$(diskutil info "$DEVICE" 2>/dev/null)" || fail "diskutil cannot inspect device: $DEVICE"
  if printf '%s\n' "$info" | awk -F: '/Mounted:/ { gsub(/^[ \t]+|[ \t]+$/, "", $2); if ($2 == "Yes") found=1 } END { exit found ? 0 : 1 }'; then
    fail "device is mounted according to diskutil: $DEVICE"
  fi
  local base
  base="$(basename "$DEVICE")"
  base="${base#r}"
  if diskutil list "$base" 2>/dev/null | awk '/[[:space:]]\/[^[:space:]]*/ { found=1 } END { exit found ? 0 : 1 }'; then
    fail "device or one of its children has a mountpoint according to diskutil: $DEVICE"
  fi
}

case "$(uname -s)" in
  Linux)
    command -v findmnt >/dev/null 2>&1 || fail "findmnt is required to check mounted filesystems on Linux"
    command -v lsblk >/dev/null 2>&1 || fail "lsblk is required to check mounted child devices on Linux"
    check_mounted_linux
    ;;
  Darwin)
    command -v diskutil >/dev/null 2>&1 || fail "diskutil is required to check mounted filesystems on macOS"
    check_mounted_macos
    ;;
  *)
    fail "unsupported OS for mount-safety checks: $(uname -s)"
    ;;
esac

printf 'About to overwrite %s bytes on %s using %s repeated 4MB block writes.\n' "$((BLOCK_SIZE * REPEAT_COUNT))" "$DEVICE" "$REPEAT_COUNT" >&2
printf 'Type WRITE to continue: ' >&2
read -r CONFIRM
if [ "$CONFIRM" != "WRITE" ]; then
  fail "confirmation did not match WRITE"
fi

dd if=/dev/urandom of="$TMP_BLOCK" bs="$BLOCK_SIZE" count=1 status=none

for index in $(seq 1 "$REPEAT_COUNT"); do
  dd if="$TMP_BLOCK" of="$DEVICE" bs="$BLOCK_SIZE" count=1 seek="$((index - 1))" conv=notrunc status=none
  printf 'Wrote block %s/%s\n' "$index" "$REPEAT_COUNT"
done

sync
printf 'Done.\n'
