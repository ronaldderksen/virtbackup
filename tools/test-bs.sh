#!/usr/bin/env bash
set -euo pipefail

LOG_FILE="/var/tmp/test-bs.log"
[ -e ${LOG_FILE} ] && mv ${LOG_FILE} ${LOG_FILE}.1
exec > >(tee "$LOG_FILE") 2>&1

VMS=(
  rocky10
  #win10
)

DESTINATONS=(
  nas
  filesystem
  stack
  strato
  gdrive
  #dummy
)

BS=(
  #1
  2
  4
  #8
)

EXTRA_PARAMS=( )
#EXTRA_PARAMS=( --no-restore )
#EXTRA_PARAMS=( --no-backup )

for vm in ${VMS[@]}; do
  for bs in ${BS[@]}; do
    for dest in ${DESTINATONS[@]}; do
      echo "[$(date '+%Y-%m-%d %H:%M:%S')] vm=${vm} dest=${dest}"
      [ "${dest}" = dummy ] && EXTRA_PARAMS+=( --no-restore )
      echo "+ dart run tools/backup_verify.dart --vm ${vm} --storage ${dest} ${EXTRA_PARAMS[@]} --block-size-mb ${bs}"
      dart run tools/backup_verify.dart --vm ${vm} --storage ${dest} ${EXTRA_PARAMS[@]} --block-size-mb ${bs} || true
    done
  done
done
