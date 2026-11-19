# 0007. Rejection is terminal and publishes no OrderCancelled (resolution A)

## Status
Accepted (open question A).

## Context
Spec section 7.2 says a rejected order ends REJECTED with "no compensation needed". Section 6 lists `RESTAURANT_REJECTED` as an `OrderCancelled` reason, which would mean a cancel event after a reject.

## Decision
- `OrderRejected` moves PLACED to REJECTED. REJECTED is terminal.
- No `OrderCancelled` is published for a rejected order.
- `RESTAURANT_REJECTED` stays in the `CancelReason` enum, marked "reserved, never emitted in v1", so the wire format matches the spec.
- The restaurant creates no ticket for a rejected order, and Delivery ignores `OrderRejected`. Nothing needs undoing.

## Consequences
- Fewer events and a simpler flow.
- Delivery may already hold a PENDING row from `OrderPlaced`. It is left alone, because no driver is assigned before `OrderAccepted`.
- `RestaurantRejectsE2E` and `OrderSagaIT` check that nothing is published after a reject.
- If a later version needs the event, the enum value is already there.
