#!/usr/bin/env bash
set -euo pipefail

LOG_FILE="/var/tmp/loop.log"
[ -e ${LOG_FILE} ] && mv ${LOG_FILE} ${LOG_FILE}.1
exec > >(tee "$LOG_FILE") 2>&1

VMS=(
  #rocky10-2
  #rocky10
  win10
  win11
)

DESTINATONS=(
  nas
  filesystem
  #stack
  #strato
  #gdrive
)

EXTRA_PARAMS=( )
#EXTRA_PARAMS=( --fresh )

echodo() {
  echo "+ ${@}"
  eval "${@}"
}

for vm in ${VMS[@]}; do
  for dest in ${DESTINATONS[@]}; do
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] vm=${vm} dest=${dest}"
    [ "${dest}" = dummy ] && EXTRA_PARAMS+=( --no-restore )
    echo ; echodo dart run tools/backup_verify.dart --vm ${vm} --storage ${dest} ${EXTRA_PARAMS[@]}
  done
done
