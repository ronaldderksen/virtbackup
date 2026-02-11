#!/usr/bin/env bash
set -euo pipefail

LOG_FILE="/var/tmp/loop.log"
exec > >(tee "$LOG_FILE") 2>&1

VMS=(
  #rocky10-2
  #rocky10
  win10
)

DRIVERS=(
  sftp
  #filesystem
  gdrive
  #dummy
)

EXTRA_PARAMS=( )
EXTRA_PARAMS=( --fresh )

for vm in ${VMS[@]}; do
  for driver in ${DRIVERS[@]}; do
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] vm=${vm} driver=${driver}"
    [ "${driver}" = dummy ] && EXTRA_PARAMS+=( --no-restore )
    dart run tools/backup_verify.dart --vm ${vm} --driver ${driver} ${EXTRA_PARAMS[@]} || true
  done
done
