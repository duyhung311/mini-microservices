#!/usr/bin/env bash
# Verifies Kafka topics, partitions, replication and broker config.
# Usage: bash scripts/check-topics.sh [--prodlike]
set -euo pipefail
export MSYS_NO_PATHCONV=1

cd "$(dirname "$0")/.."

COMPOSE=(docker compose)
SVC=kafka
RF=1
PRODLIKE=0
if [[ "${1:-}" == "--prodlike" ]]; then
  PRODLIKE=1
  COMPOSE=(docker compose -f docker-compose.prodlike.yml)
  SVC=kafka-1
  RF=3
fi
KT=/opt/kafka/bin/kafka-topics.sh
KC=/opt/kafka/bin/kafka-configs.sh
FAILS=0
pass() { echo "PASS: $*"; }
fail() { echo "FAIL: $*"; FAILS=$((FAILS + 1)); }

# Expected "name partitions" lines.
expected() {
  if (( PRODLIKE )); then
    for s in order restaurant delivery; do echo "$s.events 3"; done
    return
  fi
  for s in order restaurant delivery; do echo "$s.events 3"; done
  local p src grp i
  for p in order:restaurant-service order:delivery-service restaurant:order-service \
           restaurant:delivery-service delivery:order-service delivery:restaurant-service; do
    src=${p%%:*}; grp=${p#*:}
    for i in 0 1 2; do echo "$src.events.$grp.retry-$i 3"; done
    echo "$src.events.$grp.dlt 1"
  done
}
EXPECTED_COUNT=$(expected | wc -l | tr -d ' ')

# 1. Wait up to 90 s for kafka-init to exit.
deadline=$((SECONDS + 90))
state=""
while (( SECONDS < deadline )); do
  state=$("${COMPOSE[@]}" ps -a kafka-init --format '{{.State}} {{.ExitCode}}' 2>/dev/null | tr -d '\r' | head -n1 || true)
  [[ "$state" == exited* ]] && break
  sleep 2
done
if [[ "$state" == "exited 0" ]]; then
  pass "kafka-init finished (exited 0)"
else
  fail "kafka-init did not finish with exit 0 within 90 s (state: '${state:-none}')"
fi

# 2. Describe topics.
DESC=$("${COMPOSE[@]}" exec -T "$SVC" "$KT" --bootstrap-server localhost:9092 --describe 2>&1 | tr -d '\r') || {
  echo "$DESC"; fail "kafka-topics --describe failed"; echo "RESULT: FAIL ($FAILS failed)"; exit 1; }
# "name partitions rf" per topic, internal topics excluded.
ACTUAL=$(printf '%s\n' "$DESC" | grep -E '^Topic: ' | grep -v -E '^Topic: __' \
  | sed -E 's/^Topic: ([^[:space:]]+).*PartitionCount: ([0-9]+).*ReplicationFactor: ([0-9]+).*/\1 \2 \3/' | sort)

if [[ "$(printf '%s\n' "$ACTUAL" | awk 'NF{print $1}')" == "$(expected | awk '{print $1}' | sort)" ]]; then
  pass "exactly $EXPECTED_COUNT expected topics exist"
else
  fail "topic name set differs from expected"
  diff <(printf '%s\n' "$ACTUAL" | awk 'NF{print $1}') <(expected | awk '{print $1}' | sort) || true
fi

bad_parts=0
bad_rf=0
while read -r name parts; do
  line=$(printf '%s\n' "$ACTUAL" | awk -v n="$name" '$1==n')
  [[ -z "$line" ]] && continue
  ap=$(echo "$line" | awk '{print $2}'); ar=$(echo "$line" | awk '{print $3}')
  if [[ "$ap" != "$parts" ]]; then fail "$name partitions=$ap expected $parts"; bad_parts=1; fi
  if [[ "$ar" != "$RF" ]]; then fail "$name RF=$ar expected $RF"; bad_rf=1; fi
done < <(expected)
(( bad_parts )) || pass "partition counts (3 main/retry, 1 DLT)"
(( bad_rf )) || pass "replication factor $RF on every topic"

# 3. Broker config / min ISR.
if (( PRODLIKE )); then
  for t in order.events restaurant.events delivery.events; do
    cfg=$("${COMPOSE[@]}" exec -T "$SVC" "$KC" --bootstrap-server localhost:9092 --describe \
      --entity-type topics --entity-name "$t" 2>&1 | tr -d '\r') || true
    if grep -q 'min.insync.replicas=2' <<<"$cfg"; then
      pass "$t min.insync.replicas=2"
    else
      fail "$t min.insync.replicas=2 not found"; echo "$cfg"
    fi
  done
fi
cfg=$("${COMPOSE[@]}" exec -T "$SVC" "$KC" --bootstrap-server localhost:9092 --describe \
  --entity-type brokers --entity-name 1 --all 2>&1 | tr -d '\r') || true
if grep -q 'auto.create.topics.enable=false' <<<"$cfg"; then
  pass "auto.create.topics.enable=false"
else
  fail "auto.create.topics.enable=false not found"
fi

if (( FAILS )); then
  echo "RESULT: FAIL ($FAILS failed)"
  exit 1
fi
echo "RESULT: ALL CHECKS PASSED"
