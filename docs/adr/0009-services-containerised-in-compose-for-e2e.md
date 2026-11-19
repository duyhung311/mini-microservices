# 0009. Services run containerised in Compose for E2E tests (resolution D)

## Status
Accepted (open question D).

## Context
Spec section 14 wants end-to-end tests of the whole saga. The services could run on the host (as `java -jar`) or in containers. Eureka and the gateway need addresses that resolve for every caller. Running 3 restaurant instances on the host needs 3 different ports.

## Decision
- Compose profile `apps` builds and runs discovery-server, api-gateway, order-service, restaurant-service and delivery-service from `docker/service.Dockerfile`.
- Only the gateway (8000) and Eureka (8761) are published. The 3 services have no host ports, so `--scale restaurant-service=3` just works.
- `*E2E` tests live in `e2e-tests` and run only with `-Pe2e`. They talk to the gateway on `http://localhost:8000`.
- Per-service `*IT` tests use Testcontainers inside the JVM. Host mode (`java -jar`, ports 8081 to 8083) is for the milestone checks M2 to M8.
- Host mode and Compose mode must not be mixed: Eureka would advertise addresses that other processes cannot reach.

## Consequences
- E2E tests exercise the real images, the gateway and discovery.
- A full run needs about 5 GB of RAM (each app has `mem_limit: 512m`).
- Jars must be built before `docker compose --build` (`./mvnw -B -q package -DskipTests`).
- Prometheus `eureka_sd_configs` can discover every replica.
