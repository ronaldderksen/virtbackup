#!/usr/bin/env bash
set -euo pipefail

LOG_FILE="/var/tmp/loop.log"
[ -e ${LOG_FILE} ] && mv ${LOG_FILE} ${LOG_FILE}.1
exec > >(tee "$LOG_FILE") 2>&1

VMS=(
  rocky10
  #win10
)

DESTINATONS=(
  #nas
  #filesystem
  #stack
  strato
  #dest_dummy
  #dest_gdrive
)

BS=(
  1
  2
  4
  8
)

EXTRA_PARAMS=( )
EXTRA_PARAMS=( --no-restore )
EXTRA_PARAMS=( --no-backup )
#EXTRA_PARAMS=( --fresh )

for dest in ${DESTINATONS[@]}; do
  for vm in ${VMS[@]}; do
    for bs in ${BS[@]}; do
      echo "[$(date '+%Y-%m-%d %H:%M:%S')] vm=${vm} dest=${dest}"
      [ "${dest}" = dummy ] && EXTRA_PARAMS+=( --no-restore )
      echo "+ dart run tools/backup_verify.dart --vm ${vm} --dest ${dest} ${EXTRA_PARAMS[@]} --block-size-mb ${bs}"
      dart run tools/backup_verify.dart --vm ${vm} --dest ${dest} ${EXTRA_PARAMS[@]} --block-size-mb ${bs}
    done
  done
done
