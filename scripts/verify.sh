#!/usr/bin/env bash
# Project verify command. Usage: bash scripts/verify.sh [--full] [--down]
set -euo pipefail
# Do NOT export MSYS_NO_PATHCONV here: it stops Git Bash converting the /c/... classpath that mvnw
# passes to the Windows JVM ("Could not find or load main class ...Launcher"). Scripts that run
# `docker compose exec` with container paths (check-topics.sh etc.) set it themselves.

cd "$(dirname "$0")/.."

FULL=0
DOWN=0
for arg in "$@"; do
  case "$arg" in
    --full) FULL=1 ;;
    --down) DOWN=1 ;;
    *) echo "unknown argument: $arg (usage: verify.sh [--full] [--down])" >&2; exit 2 ;;
  esac
done

banner() { echo; echo "=== STEP $1: $2 ==="; }

banner 1 "docker info"
docker info > /dev/null
echo "docker is reachable"

banner 2 "CR check (no CR in scripts, mvnw, docker, infra)"
paths=()
for p in scripts mvnw docker infra; do [[ -e "$p" ]] && paths+=("$p"); done
if (( ${#paths[@]} )); then
  # Git for Windows quirks: grep needs -U (binary mode) or it strips CR and never matches, and
  # $'\r' written inside $( ) expands to an empty pattern (which matches every file), so the CR
  # is put in a variable first.
  cr=$'\r'
  cr_files=$(grep -rlIU "$cr" "${paths[@]}" || true)
  if [[ -n "$cr_files" ]]; then
    echo "FAIL: files contain CR characters:" >&2
    echo "$cr_files" >&2
    exit 1
  fi
fi
echo "no CR characters found"

banner 3 "./mvnw -B verify"
./mvnw -B verify

banner 4 "KafkaTemplate only in outbox packages and KafkaConfig"
dirs=()
for d in */src/main/java; do [[ -d "$d" ]] && dirs+=("$d"); done
offenders=""
if (( ${#dirs[@]} )); then
  offenders=$(grep -rln KafkaTemplate "${dirs[@]}" | grep -v -e /outbox/ -e KafkaConfig || true)
fi
if [[ -n "$offenders" ]]; then
  echo "FAIL: KafkaTemplate used outside outbox/KafkaConfig:" >&2
  echo "$offenders" >&2
  exit 1
fi
echo "no KafkaTemplate offenders"

banner 5 "docker compose up -d && check-topics"
docker compose up -d
bash scripts/check-topics.sh

if (( FULL )); then
  banner 6 "build jars, compose apps + obs profiles up (restaurant-service x3)"
  ./mvnw -B -q package -DskipTests
  docker compose --profile apps --profile obs up -d --build --scale restaurant-service=3

  banner 7 "wait for stack"
  bash scripts/wait-for-stack.sh

  banner 8 "consumer groups"
  bash scripts/check-consumer-groups.sh restaurant-service 3

  banner 9 "e2e tests"
  ./mvnw -B -Pe2e verify -pl e2e-tests
fi

if (( DOWN )); then
  banner "down" "docker compose down"
  docker compose --profile apps --profile obs down
fi

echo
echo "VERIFY OK"
