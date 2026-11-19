# 0013. Approved additive columns: `correlation_id` on tickets and deliveries

## Status
Accepted (2026-11-19). Supersedes the undocumented additions made in the restaurant and delivery V1 migrations.

## Context
spec §9 defines `kitchen_tickets` and `deliveries` without a correlation id. But several events are not
triggered by an incoming Kafka event: `POST /tickets/{id}/ready`, the auto-advance schedulers, pickup and
deliver, and `PATCH /drivers` (DRIVER_CANCELLED). Their envelopes have `causationId: null`, yet they must
keep the saga's `correlationId` so the order's events stay linked and the saga stays one Jaeger trace
(see [ADR 0011](0011-tracing-via-correlation-id-and-outbox-traceparent.md)).

The first implementation also stored the triggering event id (`kitchen_tickets.cause_event_id`,
`deliveries.last_event_id`) to chain REST events with `Envelopes.next`. The plan instead requires null
causation for REST- and scheduler-originated events, so those columns ended up written but never read.
An independent review flagged all four columns as not allowed by the plan (criterion 13).

## Decision
- Keep `kitchen_tickets.correlation_id` and `deliveries.correlation_id` (`uuid not null`), copied from the
  first event the service sees for the order, as approved additions.
- Drop `kitchen_tickets.cause_event_id` and `deliveries.last_event_id` with new `V5__…` Flyway migrations.
  Applied migrations are never edited.
- `kitchen_tickets.restaurant_id` becomes nullable (restaurant `V5`), so a CANCELLED tombstone row can be stored
  when `OrderCancelled` arrives before `OrderPlaced` ([ADR 0006](0006-ticket-final-state-cancelled.md)).
- The schema ITs list exactly the spec columns plus the approved additions.

## Consequences
- Every event a service publishes can carry the order's correlationId without a lookup in another service.
- The V1 migrations still create the dropped columns, and V5 removes them. A fresh database ends in the same
  schema as an upgraded one.
