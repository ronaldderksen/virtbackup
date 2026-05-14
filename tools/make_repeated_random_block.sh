#!/usr/bin/env bash
set -euo pipefail

OUT_FILE="${1:-repeated-random-4mb.bin}"
BLOCK_SIZE=$((4 * 1024 * 1024))
REPEAT_COUNT=50
TMP_BLOCK="$(mktemp "${TMPDIR:-/tmp}/virtbackup-random-block.XXXXXX")"

cleanup() {
  rm -f "$TMP_BLOCK"
}
trap cleanup EXIT

dd if=/dev/urandom of="$TMP_BLOCK" bs="$BLOCK_SIZE" count=1 status=none
: > "$OUT_FILE"

for _ in $(seq 1 "$REPEAT_COUNT"); do
  cat "$TMP_BLOCK" >> "$OUT_FILE"
done

printf 'Wrote %s bytes to %s\n' "$((BLOCK_SIZE * REPEAT_COUNT))" "$OUT_FILE"
