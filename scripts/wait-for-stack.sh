#!/usr/bin/env bash
# Waits (max 180 s) until the Compose `apps` stack is ready.
# Usage: bash scripts/wait-for-stack.sh [restaurantInstances=3]
# Checks: Eureka lists ORDER-SERVICE x1, RESTAURANT-SERVICE xN, DELIVERY-SERVICE x1, API-GATEWAY x1;
# gateway /actuator/health is UP; GET localhost:8000/restaurants returns 200.
# With the `obs` profile running also: Prometheus /-/ready, all active Prometheus targets up (N+5), and the Jaeger
# API lists api-gateway, order-service, restaurant-service and delivery-service.
set -uo pipefail

N=${1:-3}
EUREKA=${EUREKA_BASE:-http://localhost:8761}
GATEWAY=${GATEWAY_BASE:-http://localhost:8000}
TIMEOUT=180
PROM=${PROM_BASE:-http://localhost:9090}
JAEGER=${JAEGER_BASE:-http://localhost:16686}

# True when the Compose `obs` profile has a running prometheus container.
obs_running() {
  [[ -n "$(docker compose --profile obs ps -q --status running prometheus 2>/dev/null)" ]]
}

APPS_JSON=""
count_app() { printf '%s' "$APPS_JSON" | grep -o "\"app\":\"$1\"" | wc -l | tr -d ' \r'; }

# Prints nothing and returns 0 when all checks pass; otherwise prints the first failing check.
check_all() {
  APPS_JSON=$(curl -s -m 5 -H "Accept: application/json" "$EUREKA/eureka/apps" 2>/dev/null) || true
  local spec name want got
  for spec in "ORDER-SERVICE:1" "RESTAURANT-SERVICE:$N" "DELIVERY-SERVICE:1" "API-GATEWAY:1"; do
    name=${spec%%:*}; want=${spec#*:}
    got=$(count_app "$name")
    if [[ "$got" != "$want" ]]; then
      echo "eureka: $name has $got instance(s), expected $want"; return 1
    fi
  done
  local health
  health=$(curl -s -m 5 "$GATEWAY/actuator/health" 2>/dev/null) || true
  if ! grep -q '"status":"UP"' <<<"$health"; then
    echo "gateway /actuator/health is not UP (got: '${health:0:80}')"; return 1
  fi
  local code
  code=$(curl -s -m 5 -o /dev/null -w '%{http_code}' "$GATEWAY/restaurants" 2>/dev/null) || true
  if [[ "$code" != "200" ]]; then
    echo "GET $GATEWAY/restaurants returned '$code', expected 200"; return 1
  fi
  # The gateway's load-balancer cache can still hold an older, shorter instance list after Eureka
  # already shows N; the stack is only ready once the gateway actually spreads calls (N > 1).
  if (( N > 1 )); then
    local ids
    ids=$(for _ in $(seq 1 12); do
            curl -s -m 5 -D - -o /dev/null "$GATEWAY/restaurants" 2>/dev/null | tr -d '\r' \
              | grep -i '^x-instance-id:' | cut -d' ' -f2
          done | sort -u | grep -c . || true)
    if (( ids < 2 )); then
      echo "gateway spread 12 calls over $ids restaurant instance(s), expected >= 2"; return 1
    fi
  fi
  # M10 checks (only when the `obs` profile is running; otherwise skipped)
  if obs_running; then
    local ready
    ready=$(curl -s -m 5 "$PROM/-/ready" 2>/dev/null) || true
    if ! grep -qi 'ready' <<<"$ready"; then
      echo "prometheus $PROM/-/ready is not ready (got: '${ready:0:60}')"; return 1
    fi
    local targets up down
    targets=$(curl -s -m 5 "$PROM/api/v1/targets?state=active" 2>/dev/null) || true
    up=$(printf '%s' "$targets" | grep -o '"health":"up"' | wc -l | tr -d ' \r')
    down=$(printf '%s' "$targets" | grep -o '"health":"\(down\|unknown\)"' | wc -l | tr -d ' \r')
    # Eureka apps (1 order + N restaurant + 1 delivery + 1 gateway) + discovery-server + kafka-exporter
    local want=$((N + 5))
    if [[ "$down" != "0" || "$up" != "$want" ]]; then
      echo "prometheus targets: $up up, $down down/unknown, expected $want up and 0 down"; return 1
    fi
    # Warm-up traffic so every service has exported at least one span (GET /restaurants was sent above)
    curl -s -m 5 -o /dev/null "$GATEWAY/drivers" 2>/dev/null || true
    curl -s -m 5 -o /dev/null "$GATEWAY/orders/00000000-0000-0000-0000-000000000000" 2>/dev/null || true
    local services svc
    services=$(curl -s -m 5 "$JAEGER/api/services" 2>/dev/null) || true
    for svc in api-gateway order-service restaurant-service delivery-service; do
      if ! grep -q "\"$svc\"" <<<"$services"; then
        echo "jaeger /api/services does not list $svc yet (got: '${services:0:100}')"; return 1
      fi
    done
  fi
  return 0
}

start=$SECONDS
last=""
while :; do
  if last=$(check_all); then
    echo "PASS: stack ready after $((SECONDS - start)) s (ORDER x1, RESTAURANT x$N, DELIVERY x1, API-GATEWAY x1, gateway UP, /restaurants 200)"
    exit 0
  fi
  if (( SECONDS - start >= TIMEOUT )); then
    echo "FAIL: stack not ready after ${TIMEOUT} s. Failed check: $last" >&2
    exit 1
  fi
  sleep 3
done
