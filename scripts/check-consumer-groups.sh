#!/usr/bin/env bash
# Asserts that a consumer group's order.events partitions are spread over exactly N distinct hosts
# (one partition each). Each restaurant instance runs 2 listener containers (order.events and
# delivery.events) in the same group, so raw members are 2N; only order.events owners are counted.
# Usage: bash scripts/check-consumer-groups.sh <group> <expectedInstances>
set -euo pipefail
export MSYS_NO_PATHCONV=1

cd "$(dirname "$0")/.."

if (( $# != 2 )); then
  echo "usage: check-consumer-groups.sh <group> <expectedInstances>" >&2
  exit 2
fi
GROUP=$1
EXPECTED=$2
TOPIC=order.events

OUT=$(docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group "$GROUP" 2>&1 | tr -d '\r') || {
  echo "$OUT"; echo "FAIL: kafka-consumer-groups --describe failed"; exit 1; }
echo "$OUT"

# Columns: GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
ROWS=$(printf '%s\n' "$OUT" | awk -v t="$TOPIC" '$2==t && $7!="-" {print $3, $8}')
PARTS=$(printf '%s\n' "$ROWS" | awk 'NF' | wc -l | tr -d ' ')
HOSTS=$(printf '%s\n' "$ROWS" | awk 'NF{print $2}' | sort -u | wc -l | tr -d ' ')
MAXPER=$(printf '%s\n' "$ROWS" | awk 'NF{print $2}' | sort | uniq -c | awk 'BEGIN{m=0}{if($1>m)m=$1}END{print m}')
echo "group=$GROUP topic=$TOPIC owned-partitions=$PARTS distinct-hosts=$HOSTS max-partitions-per-host=$MAXPER"

if [[ "$HOSTS" == "$EXPECTED" && "$PARTS" == "$EXPECTED" && "$MAXPER" == "1" ]]; then
  echo "PASS: $TOPIC partitions are owned by $EXPECTED distinct hosts, one partition each"
else
  echo "FAIL: expected $EXPECTED hosts owning one $TOPIC partition each" >&2
  exit 1
fi
