#!/usr/bin/env bash
# Demo scenarios for the food-delivery saga. Everything goes through the gateway.
# Usage: bash scripts/demo.sh happy|reject|nodriver|cancel|poison
# Needs the Compose stack: docker compose --profile apps --profile obs up -d --build --scale restaurant-service=3
# Uses only curl, grep, sed and cut (no python, no jq).
set -uo pipefail
# MSYS_NO_PATHCONV=1 is set only on the docker compose exec calls: exported globally it stops Git Bash
# converting "-o /dev/null" for curl.exe (curl then exits 23 and every check reports 000).

cd "$(dirname "$0")/.."

GW=${GATEWAY_BASE:-http://localhost:8000}
CUSTOMER=99999999-9999-9999-9999-999999999999
PASTA=11111111-1111-1111-1111-111111111111
NIGHT_GRILL=11111111-1111-1111-1111-111111111112
MARGHERITA=22222222-2222-2222-2222-000000000001
DRIVERS=(33333333-3333-3333-3333-000000000001 33333333-3333-3333-3333-000000000002
         33333333-3333-3333-3333-000000000003 33333333-3333-3333-3333-000000000004
         33333333-3333-3333-3333-000000000005)
MAX_WAIT=60

step() { echo; echo "--> $*"; }
die()  { echo "DEMO FAILED: $*" >&2; exit 1; }

# GET a path and print the body.
get() { curl -s -m 10 "$GW$1"; }
# POST without a body. Prints only the HTTP status code.
post() { curl -s -m 10 -o /dev/null -w '%{http_code}' -X POST "$GW$1"; }
# First string value of a JSON key, read from stdin.
jstr() { grep -o "\"$1\":\"[^\"]*\"" | head -1 | cut -d'"' -f4; }

order_status()    { get "/orders/$1" | jstr status; }
order_history()   { get "/orders/$1/history" | grep -o '"toStatus":"[^"]*"' | cut -d'"' -f4 | tr '\n' ' ' | sed 's/ $//; s/ / > /g'; }
ticket_status()   { get "/tickets/$1" | jstr status; }
delivery_status() { get "/deliveries/$1" | jstr status; }

# Helpers for wait_for: they read the order id from $OID.
cur_order()    { order_status "$OID"; }
cur_ticket()   { ticket_status "$OID"; }
cur_delivery() { delivery_status "$OID"; }

# wait_for <description> <function that prints a value> <wanted value>; polls for at most MAX_WAIT seconds.
wait_for() {
  local what=$1 cmd=$2 want=$3 got="" t=0
  while (( t < MAX_WAIT )); do
    got=$($cmd)
    if [[ "$got" == "$want" ]]; then echo "    $what = $got (after ${t}s)"; return 0; fi
    sleep 1; t=$((t + 1))
  done
  echo "    $what is '$got', wanted '$want' after ${MAX_WAIT}s" >&2
  return 1
}

check_gateway() {
  local code
  code=$(curl -s -m 5 -o /dev/null -w '%{http_code}' "$GW/restaurants" 2>/dev/null) || code=000
  [[ "$code" == "200" ]] || die "gateway $GW/restaurants returned $code. Start the stack: docker compose --profile apps --profile obs up -d --build --scale restaurant-service=3, then bash scripts/wait-for-stack.sh"
}

# place_order <restaurantId>: sets OID and CORR.
place_order() {
  local body resp
  body="{\"customerId\":\"$CUSTOMER\",\"restaurantId\":\"$1\",\"items\":[{\"menuItemId\":\"$MARGHERITA\",\"quantity\":2,\"unitPrice\":12.50}],\"deliveryAddress\":\"5 Demo Road\"}"
  resp=$(curl -s -m 10 -X POST -H 'Content-Type: application/json' -d "$body" "$GW/orders")
  OID=$(printf '%s' "$resp" | jstr orderId)
  CORR=$(printf '%s' "$resp" | jstr correlationId)
  [[ -n "$OID" ]] || die "POST /orders did not return an orderId. Response: $resp"
  echo "    orderId       = $OID"
  echo "    correlationId = $CORR"
  echo "    Jaeger trace  = http://localhost:16686/trace/$(printf '%s' "$CORR" | tr -d '-')"
}

patch_all_drivers() {
  local d
  for d in "${DRIVERS[@]}"; do
    curl -s -m 10 -o /dev/null -X PATCH -H 'Content-Type: application/json' -d "{\"status\":\"$1\"}" "$GW/drivers/$d" || true
  done
}

scenario_happy() {
  step "1. Place an order at Pasta Place (POST /orders)"
  place_order "$PASTA"
  step "2. Restaurant accepts, delivery assigns a driver"
  wait_for "ticket" cur_ticket ACCEPTED || return 1
  wait_for "delivery" cur_delivery ASSIGNED || return 1
  step "3. Kitchen finishes the food (POST /tickets/{id}/ready)"
  echo "    HTTP $(post "/tickets/$OID/ready")"
  wait_for "delivery" cur_delivery READY_FOR_PICKUP || return 1
  step "4. Driver picks up (POST /deliveries/{id}/pickup)"
  echo "    HTTP $(post "/deliveries/$OID/pickup")"
  wait_for "order" cur_order OUT_FOR_DELIVERY || return 1
  step "5. Driver delivers (POST /deliveries/{id}/deliver)"
  echo "    HTTP $(post "/deliveries/$OID/deliver")"
  wait_for "order" cur_order DELIVERED || return 1
  step "Order history"
  echo "    $(order_history "$OID")"
}

scenario_reject() {
  step "1. Place an order at Night Grill (seeded as closed)"
  place_order "$NIGHT_GRILL"
  step "2. Restaurant rejects with CLOSED. The order ends REJECTED (terminal, no OrderCancelled)"
  wait_for "order" cur_order REJECTED || return 1
  step "Order history"
  echo "    $(order_history "$OID")"
}

nodriver_cleanup() {
  echo; echo "--> Restoring all drivers to AVAILABLE"
  patch_all_drivers AVAILABLE
}
scenario_nodriver() {
  trap nodriver_cleanup EXIT
  step "1. PATCH all 5 drivers OFFLINE"
  patch_all_drivers OFFLINE
  step "2. Place an order at Pasta Place"
  place_order "$PASTA"
  step "3. Delivery fails (NO_DRIVER_AVAILABLE), order service cancels, restaurant cancels the ticket"
  wait_for "order" cur_order CANCELLED || return 1
  wait_for "ticket" cur_ticket CANCELLED || return 1
  wait_for "delivery" cur_delivery FAILED || return 1
  step "Order history"
  echo "    $(order_history "$OID")"
}

scenario_cancel() {
  step "1. Place an order at Pasta Place"
  place_order "$PASTA"
  wait_for "delivery" cur_delivery ASSIGNED || return 1
  wait_for "order" cur_order DRIVER_ASSIGNED || return 1
  step "2. Customer cancels (POST /orders/{id}/cancel). Expect 200"
  echo "    HTTP $(post "/orders/$OID/cancel")"
  wait_for "order" cur_order CANCELLED || return 1
  wait_for "ticket" cur_ticket CANCELLED || return 1
  wait_for "delivery" cur_delivery CANCELLED || return 1
  step "3. Cancel again. Expect 409"
  echo "    HTTP $(post "/orders/$OID/cancel")"
  step "Order history"
  echo "    $(order_history "$OID")"
}

scenario_poison() {
  command -v docker >/dev/null 2>&1 || die "docker is not on PATH (Git Bash does not add it by default)"
  local key="demo-poison-$(date +%s)" topic=order.events.restaurant-service.dlt
  local KB=/opt/kafka/bin out="" t=0
  step "1. Produce the non-JSON record 'this-is-not-json' to order.events with key $key"
  printf '%s:this-is-not-json\n' "$key" | MSYS_NO_PATHCONV=1 docker compose exec -T kafka "$KB/kafka-console-producer.sh" \
    --bootstrap-server localhost:9092 --topic order.events \
    --property parse.key=true --property key.separator=: || die "producer failed"
  step "2. Look for it on $topic (poison goes straight to the DLT, no retry hops)"
  while (( t < MAX_WAIT )); do
    out=$(MSYS_NO_PATHCONV=1 docker compose exec -T kafka "$KB/kafka-console-consumer.sh" \
      --bootstrap-server localhost:9092 --topic "$topic" --from-beginning \
      --property print.key=true --timeout-ms 8000 2>/dev/null | tr -d '\r')
    if grep -q "^$key" <<<"$out"; then
      echo "    found on the DLT:"
      grep "^$key" <<<"$out" | sed 's/^/      /'
      break
    fi
    t=$((t + 10))
  done
  grep -q "^$key" <<<"$out" || die "record with key $key not found on $topic after ${MAX_WAIT}s"
  step "3. The retry topic stays empty for this key"
  out=$(MSYS_NO_PATHCONV=1 docker compose exec -T kafka "$KB/kafka-console-consumer.sh" \
    --bootstrap-server localhost:9092 --topic order.events.restaurant-service.retry-0 --from-beginning \
    --property print.key=true --timeout-ms 5000 2>/dev/null | tr -d '\r')
  if grep -q "^$key" <<<"$out"; then echo "    UNEXPECTED: key found on retry-0"; else echo "    not on retry-0 (as expected)"; fi
  echo "    The ERROR log line: docker compose logs restaurant-service | grep ALERT"
}

case "${1:-}" in
  happy|reject|nodriver|cancel|poison)
    [[ "$1" == poison ]] || check_gateway
    "scenario_$1" || die "scenario '$1' did not reach the expected state"
    echo; echo "DEMO '$1' OK"
    ;;
  *)
    echo "usage: bash scripts/demo.sh happy|reject|nodriver|cancel|poison" >&2
    exit 2
    ;;
esac
