#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
Usage: tools/write_dummy_job_history.sh <count> <logs-dir>

Writes <count> dummy agent-job-*.log files to /var/VirtBackup/logs.

Example:
  tools/write_dummy_job_history.sh 1000
USAGE
}

if [[ $# -ne 1 ]]; then
  usage
  exit 64
fi

count="$1"
logs_dir="/var/VirtBackup/logs"

if ! [[ "$count" =~ ^[1-9][0-9]*$ ]]; then
  echo "count must be a positive integer." >&2
  exit 64
fi

mkdir -p "$logs_dir"

base_job_id="1779126261162"
run_id="$(date -u '+%Y%m%dT%H%M%S')-$$"
states=(success failure canceled)
types=(backup restore sanity)
vms=(rocky10 debian12 ubuntu24 win2022 postgres01 gitlab-runner k8s-control)
storages=("Filesystem" "Google Drive" "SFTP NAS" "Local RAID")
sources=(nuc04 lab-host hypervisor-01 edge-node)

json_escape() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$'\n'/\\n}"
  value="${value//$'\r'/\\r}"
  printf '%s' "$value"
}

random_timestamp() {
  local now
  local offset
  now="$(date +%s)"
  offset="$((RANDOM * 962 + RANDOM % 962))"
  date -u -r "$((now - offset))" '+%Y-%m-%dT%H:%M:%S.000' 2>/dev/null && return
  date -u -d "@$((now - offset))" '+%Y-%m-%dT%H:%M:%S.000'
}

title_for_result() {
  local type="$1"
  local state="$2"
  case "$type:$state" in
    backup:success) printf 'Backup succeeded' ;;
    backup:failure) printf 'Backup failed' ;;
    backup:canceled) printf 'Backup canceled' ;;
    restore:success) printf 'Restore succeeded' ;;
    restore:failure) printf 'Restore failed' ;;
    restore:canceled) printf 'Restore canceled' ;;
    sanity:success) printf 'Check succeeded' ;;
    sanity:failure) printf 'Check failed' ;;
    sanity:canceled) printf 'Check canceled' ;;
  esac
}

for ((index = 1; index <= count; index++)); do
  state="${states[RANDOM % ${#states[@]}]}"
  type="${types[RANDOM % ${#types[@]}]}"
  vm="${vms[RANDOM % ${#vms[@]}]}"
  storage="${storages[RANDOM % ${#storages[@]}]}"
  source_host="${sources[RANDOM % ${#sources[@]}]}"
  duration="$((30 + RANDOM % 7200))"
  size_gib="$((5 + RANDOM % 250))"
  speed_mib="$((25 + RANDOM % 950))"
  job_id="${base_job_id}-${type}-dummy-${run_id}-$(printf '%06d' "$index")"
  timestamp="$(random_timestamp)"
  title="$(title_for_result "$type" "$state")"

  case "$state" in
    success)
      message="$title"
      ;;
    failure)
      message="$title: dummy failure for GUI history testing"
      ;;
    canceled)
      message="Canceled"
      ;;
  esac

  source_label="${source_host}:${vm}"
  if [[ "$type" == "restore" ]]; then
    target="$source_label"
    source_value="$storage"
  else
    target="$storage"
    source_value="$source_label"
  fi

  extra_fields=""

  if [[ "$type" != "restore" ]]; then
    physical_gib="$((RANDOM % (size_gib + 1)))"
    extra_fields=",\"physicalTransferred\":\"${physical_gib}.0 GiB\",\"averagePhysicalSpeed\":\"$((RANDOM % 200)) MiB/s\",\"total\":\"${size_gib}.0 GiB\",\"physicalTotal\":\"${physical_gib}.0 GiB\""
  fi

  json="{\"event\":\"job_result\",\"jobId\":\"$(json_escape "$job_id")\",\"type\":\"$(json_escape "$type")\",\"state\":\"$(json_escape "$state")\",\"message\":\"$(json_escape "$message")\",\"notificationStatus\":\"$(json_escape "$state")\",\"title\":\"$(json_escape "$title")\",\"vmName\":\"$(json_escape "$vm")\",\"storage\":\"$(json_escape "$storage")\",\"source\":\"$(json_escape "$source_value")\",\"target\":\"$(json_escape "$target")\",\"durationSeconds\":$duration,\"size\":\"${size_gib}.0 GiB\",\"transferred\":\"${size_gib}.0 GiB\",\"averageSpeed\":\"${speed_mib} MiB/s\"${extra_fields}}"

  log_file="$logs_dir/agent-job-${job_id}.log"
  {
    printf '%s level=info message=dummy history prelude for %s %s\n' "$timestamp" "$type" "$state"
    printf '%s level=info message=%s\n' "$timestamp" "$json"
    printf '%s level=info message=dummy history epilogue for %s\n' "$timestamp" "$job_id"
  } >"$log_file"
done

printf 'Wrote %s dummy job history files to %s\n' "$count" "$logs_dir"
