# 0011. Tracing: correlationId = traceId, and the outbox keeps the traceparent

## Status
Accepted (M10).

## Context
One saga touches the gateway, 3 services and 3 topics over several seconds. We want one Jaeger trace per saga and one id for logs. The outbox breaks the usual trace chain: the relay publishes from a scheduler thread, not from the request thread.

## Decision
- `POST /orders` sets the order's `correlationId` to the current traceId, formatted as a UUID. It is returned in the 202 body.
- To find the trace, take the correlationId, remove the dashes, and open `http://localhost:16686/trace/<hex>`.
- Every outbox row stores a W3C `traceparent` (column added in `V4__outbox_traceparent.sql`). Its traceId equals the correlationId and its parent span is the current span.
- The relay starts its "outbox relay publish" span from the stored traceparent, so the Kafka send and the consumer spans join the same trace.
- Events that start from REST or from schedulers (cancel, timeout, auto-advance) get a short "outbox enqueue" span inside the saga trace.
- Kafka observation is on (`spring.kafka.template.observation-enabled`, `spring.kafka.listener.observation-enabled`), with OTLP export to Jaeger and sampling 1.0.
- Spans carry a `correlationId` attribute and logs carry it through MDC. An `ObservationPredicate` drops scheduler, `/actuator/**` and Eureka noise.

## Consequences
- One saga is one trace, even across the outbox delay (`SagaTracingE2E`, `OutboxTraceparentIT`).
- The correlationId is a UUID-shaped traceId, not a random v4 UUID.
- If the trace is sampled out or Jaeger is down, the saga still works. The id is still in the logs and events.
- The OTLP exporter logs errors when no collector is running.
