#!/usr/bin/env bash
set -euo pipefail

DEV="br0"
IFB="ifb0"
CHAOS_PORT="2222"
TARGET_PORT="22"

NFT_TABLE="sftpchaos"
NFT_FAMILY="inet"
RESET_MOD="100000"

usage() {
  cat <<EOF
Gebruik:
  $0 <scenario>

Scenario's:
  status        Toon huidige instellingen
  clean         Verwijder netem + reset rules

  fast          Bijna goede verbinding
  good          Lichte latency
  bad           Bad
  terrible      Terrible
  stall         Grote vertraging/timeouts
  offline       100% packet loss

  reset001      Ongeveer 0.001% TCP packet resets
  reset01       Ongeveer 0.01% TCP packet resets
  reset1        Ongeveer 0.1% TCP packet resets

  chaos001      Bad + 0.001% TCP packet resets
  chaos01       Terrible + 0.01% TCP packet resets

Dit script raakt alleen poort $CHAOS_PORT.
Zorg zelf voor een permanente forward van poort $CHAOS_PORT naar SSH/SFTP poort 22.
EOF
}

require_root() {
  [[ $EUID -eq 0 ]] || {
    echo "Run als root of met sudo"
    exit 1
  }
}

setup_ifb() {
  modprobe ifb || true

  if ! ip link show "$IFB" >/dev/null 2>&1; then
    ip link add "$IFB" type ifb
  fi

  ip link set "$IFB" up

  tc qdisc del dev "$DEV" ingress 2>/dev/null || true
  tc qdisc del dev "$DEV" root 2>/dev/null || true
  tc qdisc del dev "$IFB" root 2>/dev/null || true

  tc qdisc add dev "$DEV" ingress
  tc qdisc add dev "$DEV" root handle 1: htb default 10
  tc class add dev "$DEV" parent 1: classid 1:10 htb rate 100gbit ceil 100gbit quantum 1514
  tc class add dev "$DEV" parent 1: classid 1:20 htb rate 100gbit ceil 100gbit quantum 1514

  tc filter add dev "$DEV" parent ffff: protocol ip prio 10 u32 \
    match ip protocol 6 0xff \
    match ip dport "$CHAOS_PORT" 0xffff \
    action mirred egress redirect dev "$IFB"

  tc filter add dev "$DEV" parent ffff: protocol ip prio 11 u32 \
    match ip protocol 6 0xff \
    match ip sport "$CHAOS_PORT" 0xffff \
    action mirred egress redirect dev "$IFB"

  tc filter add dev "$DEV" parent 1: protocol ip prio 10 u32 \
    match ip protocol 6 0xff \
    match ip dport "$CHAOS_PORT" 0xffff \
    flowid 1:20

  tc filter add dev "$DEV" parent 1: protocol ip prio 11 u32 \
    match ip protocol 6 0xff \
    match ip sport "$CHAOS_PORT" 0xffff \
    flowid 1:20
}

apply_netem() {
  setup_ifb
  tc qdisc add dev "$IFB" root netem "$@"
  tc qdisc add dev "$DEV" parent 1:20 handle 20: netem "$@"
}

clean_netem() {
  tc qdisc del dev "$DEV" ingress 2>/dev/null || true
  tc qdisc del dev "$DEV" root 2>/dev/null || true
  tc qdisc del dev "$IFB" root 2>/dev/null || true
}

clean_resets() {
  nft delete table "$NFT_FAMILY" "$NFT_TABLE" 2>/dev/null || true
}

apply_resets() {
  local percent="$1"

  clean_resets
  nft add table "$NFT_FAMILY" "$NFT_TABLE"

  nft "add chain $NFT_FAMILY $NFT_TABLE input { type filter hook input priority 0; policy accept; }"
  nft "add chain $NFT_FAMILY $NFT_TABLE output { type filter hook output priority 0; policy accept; }"

  # Reset alleen verbindingen die oorspronkelijk naar de chaos-poort gingen.
  nft add rule "$NFT_FAMILY" "$NFT_TABLE" input \
    tcp dport "$TARGET_PORT" ct original proto-dst "$CHAOS_PORT" numgen random mod "$RESET_MOD" "<" "$percent" \
    reject with tcp reset

  # Reset server-antwoorden alleen voor verbindingen die via de chaos-poort binnenkwamen.
  nft add rule "$NFT_FAMILY" "$NFT_TABLE" output \
    tcp sport "$TARGET_PORT" ct original proto-dst "$CHAOS_PORT" numgen random mod "$RESET_MOD" "<" "$percent" \
    reject with tcp reset
}

clean_all() {
  clean_netem
  clean_resets
  echo "SFTP chaos cleanup gedaan voor $DEV / $IFB / poort $CHAOS_PORT"
}

status() {
  echo "=== tc op $DEV ==="
  tc qdisc show dev "$DEV" || true
  tc -s class show dev "$DEV" || true
  tc -s filter show dev "$DEV" parent ffff: || true
  tc -s filter show dev "$DEV" parent 1: || true

  echo
  echo "=== tc op $IFB ==="
  tc -s qdisc show dev "$IFB" || true

  echo
  echo "=== nftables $NFT_TABLE ==="
  nft list table "$NFT_FAMILY" "$NFT_TABLE" 2>/dev/null || echo "Geen reset rules actief"
}

require_root

SCENARIO="${1:-}"

case "$SCENARIO" in
  status)
    status
    ;;

  clean|reset)
    clean_all
    ;;

  fast)
    clean_resets
    apply_netem delay 2ms 1ms
    ;;

  good)
    clean_resets
    apply_netem delay 10ms 2ms
    ;;

  bad)
    clean_resets
    apply_netem delay 50ms 10ms loss 0.05%
    ;;

  terrible)
    clean_resets
    apply_netem delay 120ms 60ms loss 2% duplicate 0.5% reorder 1% 50%
    ;;

  stall)
    clean_resets
    apply_netem delay 5s 2s distribution normal loss 5%
    ;;

  offline)
    clean_resets
    apply_netem loss 100%
    ;;

  reset001)
    clean_netem
    apply_resets 1
    ;;

  reset01)
    clean_netem
    apply_resets 10
    ;;

  reset1)
    clean_netem
    apply_resets 100
    ;;

  chaos001)
    apply_resets 1
    apply_netem delay 50ms 10ms loss 0.05%
    ;;

  chaos01)
    apply_resets 10
    apply_netem delay 120ms 60ms loss 2% duplicate 0.5% reorder 1% 50%
    ;;

  *)
    usage
    exit 1
    ;;
esac

echo "Scenario '$SCENARIO' actief voor SFTP/SSH poort $CHAOS_PORT via $DEV"
