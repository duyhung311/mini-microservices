# 0005. PREMATURE and STALE events (no order across topics)

## Status
Accepted (open question J).

## Context
Kafka orders records inside one partition only. Order Service reads `restaurant.events` and `delivery.events`, which are different topics, so `OrderReady` and `DriverAssigned` can arrive in either order. Non-blocking retry topics can also reorder records for one key. The spec says "invalid transitions are logged and ignored", but ignoring an event that is merely early would lose it and leave the saga stuck.

## Decision
`OrderStateMachine.decide(status, hasDriver, readySeen, input)` is a pure function with 3 results:
- **APPLY**: the transition is valid. Update the order, write the history row and, if needed, an outbox row.
- **PREMATURE**: the cause of this event has not been applied yet (for example `DriverAssigned` while the order is still PLACED). The handler throws `PrematureEventException`, the transaction rolls back (including the `processed_events` row) and the record goes through the retry topics.
- **STALE**: the event is behind the current state (for example `OrderReady` after CANCELLED). It is logged and recorded in `processed_events`, and nothing is retried.

Terminal states (DELIVERED, REJECTED, CANCELLED) make every input STALE. Any (state, event) pair not listed as APPLY or PREMATURE is STALE.

There are two valid happy-path histories: `DRIVER_ASSIGNED` before `READY_FOR_PICKUP`, or the other way round.

## Consequences
- The saga converges whichever way the events arrive (`OrderSagaIT`, `OrderPrematureEventIT`, `OrderingIT` with 3 instances and 20 orders).
- A PREMATURE event that never becomes valid ends on the DLT with an ALERT, so it is visible.
- Retry delays (1 s, 5 s, 25 s) are the price of a premature event.
- Known race: a customer cancel racing a pickup. The late pickup is STALE in Order and Delivery logs a WARN. It is documented, not prevented.
