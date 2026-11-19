# 0012. Postgres on host port 5433 and the provectuslabs image name

## Status
Accepted (approved by the user, 2026-11-18).

## Context
Two small deviations from the spec's literal values.
- A native Windows PostgreSQL service already owns host port 5432, so Compose Postgres could not bind it.
- The spec says `provectus/kafka-ui`. That repository does not exist on Docker Hub. The real one is `provectuslabs/kafka-ui`. The project is no longer maintained.

## Decision
- Compose publishes Postgres as `${POSTGRES_HOST_PORT:-5433}:5432`. Host-mode services default to `DB_HOST=localhost`, `DB_PORT=5433`. Inside Compose they use `postgres:5432`.
- Kafka UI is `provectuslabs/kafka-ui:v0.7.2`, with cluster name `local` and `kafka:29092`.
- If it stops working with a newer broker, the maintained fork `ghcr.io/kafbat/kafka-ui` accepts the same environment variables and can replace it.

## Consequences
- Anything that connects to Postgres from the host (psql, IDE) must use port 5433.
- Set `POSTGRES_HOST_PORT=5432` if your machine has nothing on 5432.
- The Kafka UI image is pinned, so the project does not drift.
