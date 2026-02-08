#!/usr/bin/env bash
set -euo pipefail

VM=win10
VM=rocky10

EXTRA_PARAMS=(
  --fresh
  #--no-restore
  #--driver filesystem
  #--driver dummy
  #--driver gdrive
  #--fresh
)

LOG_FILE="/var/tmp/loop.log"
exec > >(tee "$LOG_FILE") 2>&1

count=0

for count in {1..2}; do
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] loop=$count starting..."

  for driver in filesystem gdrive; do
    echo "+ dart run tools/backup_verify.dart --vm ${VM} --driver ${driver} ${EXTRA_PARAMS[@]}"
    dart run tools/backup_verify.dart --vm ${VM} --driver ${driver} ${EXTRA_PARAMS[@]}
  done

  echo "[$(date '+%Y-%m-%d %H:%M:%S')] loop=$count ok, restarting..."
  sleep 2
  [ "$count" = 2 ] && break
done
