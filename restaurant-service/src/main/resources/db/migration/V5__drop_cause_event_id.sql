-- cause_event_id was never part of the spec and nothing reads it; correlation_id stays (documented addition).
ALTER TABLE kitchen_tickets DROP COLUMN cause_event_id;

-- Tombstone rows: an OrderCancelled that arrives before its OrderPlaced is recorded as a CANCELLED ticket.
-- OrderCancelled carries no restaurantId, so the column must be nullable for such rows.
ALTER TABLE kitchen_tickets ALTER COLUMN restaurant_id DROP NOT NULL;
