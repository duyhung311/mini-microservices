# 0006. The kitchen ticket always ends CANCELLED (resolution B)

## Status
Accepted (open question B).

## Context
Spec section 3.2 gives the ticket states and says a failed delivery abandons the ticket. Section 7.3 says the restaurant cancels the ticket after `OrderCancelled`. For a "no driver" order the restaurant sees two events (`DeliveryFailed` and `OrderCancelled(NO_DRIVER)`) from two different topics, and they can arrive in either order. A final state that depends on arrival order would be non-deterministic.

## Decision
- ACCEPTED or READY becomes ABANDONED on `DeliveryFailed`. This is an intermediate step: it stops preparation, frees restaurant capacity, and `POST /tickets/{id}/ready` returns 409.
- ACCEPTED, READY or ABANDONED becomes CANCELLED on `OrderCancelled`, whatever the reason.
- CANCELLED is terminal. A `DeliveryFailed` that arrives after CANCELLED is a no-op.
- Order Service is the source of truth. Every driver failure produces `OrderCancelled(NO_DRIVER)`.
- **Tombstone** (added after review, 2026-11-19): `OrderCancelled` can overtake an `OrderPlaced` that is still
  in a retry topic. If no ticket exists yet, the restaurant stores a CANCELLED tombstone row (with
  `restaurant_id` NULL, so it never counts toward capacity). A later `OrderPlaced` for that order creates no
  ticket and publishes nothing. `GET /tickets/{orderId}` treats a tombstone as not found (404).

## Consequences
- Both arrival orders end in CANCELLED (`RestaurantCompensationIT`), including cancel-before-place
  (`RestaurantCancelBeforePlaceIT`).
- ABANDONED is visible for a short time, and only if `DeliveryFailed` arrives first.
- The restaurant depends on Order Service to finish the compensation.
