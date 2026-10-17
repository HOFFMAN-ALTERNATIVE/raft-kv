#!/usr/bin/env bash
# End-to-end fault injection against the docker compose cluster:
#   1. elect a leader, write 200 keys through random nodes (followers redirect)
#   2. SIGKILL the leader  -> a new leader must take over, no acked write lost
#   3. restart it          -> it rejoins as a follower and catches up
#   4. network-partition a follower -> the majority keeps committing, the
#      isolated node sees nothing; heal -> it catches up (InstallSnapshot)
set -euo pipefail

PORTS=(7001 7002 7003)
NODES=(n1 n2 n3)
NET="raft-kv_default"
log() { printf '\033[36m[chaos]\033[0m %s\n' "$*"; }
die() { printf '\033[31m[chaos] FAIL:\033[0m %s\n' "$*"; docker compose logs --tail=40; exit 1; }

port_of() { for i in "${!NODES[@]}"; do [[ ${NODES[$i]} == "$1" ]] && echo "${PORTS[$i]}"; done; }

leader() { # prints the node id that reports role=leader, excluding $1
  for n in "${NODES[@]}"; do
    [[ $n == "${1:-}" ]] && continue
    role=$(curl -fsS --max-time 1 "localhost:$(port_of "$n")/v1/status" 2>/dev/null | jq -r .role || true)
    [[ $role == leader ]] && { echo "$n"; return 0; }
  done
  return 1
}

wait_leader() {
  for _ in $(seq 1 100); do l=$(leader "${1:-}") && { echo "$l"; return 0; }; sleep 0.2; done
  return 1
}

put() { curl -fsS -L --max-time 5 -X PUT "localhost:$1/v1/kv/$2" --data-binary "$3" >/dev/null; }
get() { curl -fsS -L --max-time 5 "localhost:$1/v1/kv/$2" | jq -r .value; }
stale() { curl -fsS --max-time 2 "localhost:$1/v1/kv/$2?consistency=stale" 2>/dev/null | jq -r .value; }

wait_value() { # port key want
  for _ in $(seq 1 150); do [[ $(stale "$1" "$2" || true) == "$3" ]] && return 0; sleep 0.2; done
  return 1
}

L=$(wait_leader) || die "no leader elected"
log "leader is $L"

log "writing 200 keys via random nodes"
for i in $(seq 1 200); do put "${PORTS[RANDOM % 3]}" "k$i" "v$i" || die "write k$i failed"; done
[[ $(get 7001 k200) == v200 ]] || die "read-your-writes failed"

log "SIGKILL leader $L"
docker compose kill -s SIGKILL "$L" >/dev/null
L2=$(wait_leader "$L") || die "no new leader after killing $L"
log "new leader is $L2"
P2=$(port_of "$L2")
for i in 1 50 100 150 200; do [[ $(get "$P2" "k$i") == "v$i" ]] || die "acked write k$i lost after failover"; done
log "all sampled acked writes survived failover"

log "restarting $L; it must rejoin as follower and catch up"
docker compose start "$L" >/dev/null
wait_value "$(port_of "$L")" k200 v200 || die "$L did not catch up after restart"

F=""
for n in "${NODES[@]}"; do [[ $n != "$L2" ]] && F=$n && break; done
log "partitioning follower $F from the network"
docker network disconnect "$NET" "$(docker compose ps -q "$F")"
for i in $(seq 201 300); do put "$P2" "k$i" "v$i" || die "majority could not commit k$i"; done
log "majority committed 100 writes while $F was cut off"
[[ $(stale "$(port_of "$F")" k300 || true) != v300 ]] || die "partitioned node saw a write it cannot have received"

log "healing $F"
docker network connect --alias "$F" "$NET" "$(docker compose ps -q "$F")"
wait_value "$(port_of "$F")" k300 v300 || die "$F did not catch up"
for n in "${NODES[@]}"; do
  st=$(curl -fsS "localhost:$(port_of "$n")/v1/status")
  log "$n: $(jq -c '{role,term,commit_index,last_applied,snapshot_index,snapshots_installed}' <<<"$st")"
done

L3=$(wait_leader) || die "no leader at the end"
[[ $(get "$(port_of "$L3")" k1) == v1 ]] || die "k1 lost"
log "OK — leader $L3, every check passed"
