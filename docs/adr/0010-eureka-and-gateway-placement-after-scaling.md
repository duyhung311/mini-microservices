# 0010. Eureka and the gateway come after scaling (M8.5)

## Status
Accepted.

## Context
The user added service discovery and an API gateway to the spec. They could be built first, but then Kafka learning would be mixed with HTTP routing. Scaling one service (M8) is the case where discovery pays off.

## Decision
- Milestone order: M8 (consumer-group scaling) first, then M8.5 (Eureka on :8761, Spring Cloud Gateway on :8000).
- The gateway routes `/orders/**` to `lb://order-service`, `/restaurants/**` and `/tickets/**` to `lb://restaurant-service`, `/drivers/**` and `/deliveries/**` to `lb://delivery-service`.
- Eureka clients use `prefer-ip-address=true`, short leases (renew 5 s, expire 15 s) and a 5 s registry fetch. Eureka is off by default (`EUREKA_ENABLED=false`) and on in Compose.
- Restaurant adds an `X-Instance-Id` response header so load balancing is visible.
- Kafka traffic does not go through discovery. Brokers are reached through `bootstrap.servers`, and consumer groups do their own balancing.
- Spring Cloud 2025.0 renamed the gateway artifact (`spring-cloud-starter-gateway-server-webflux`) and the property prefix (`spring.cloud.gateway.server.webflux.*`).

## Consequences
- By M8.5 there are 3 restaurant instances to see in Eureka and to balance across.
- Services no longer need host ports in Compose.
- Eureka is only for HTTP. Do not expect it to show anything about Kafka.
- Two more modules and two more containers to run.
