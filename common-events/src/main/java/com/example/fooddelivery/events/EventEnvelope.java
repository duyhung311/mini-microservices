package com.example.fooddelivery.events;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

/**
 * Common envelope for every event. {@code causationId} is null for events that
 * originate from REST or a scheduler (it is still serialised, as JSON null).
 */
public record EventEnvelope(
        UUID eventId,
        String eventType,
        int eventVersion,
        Instant occurredAt,
        UUID orderId,
        UUID correlationId,
        UUID causationId,
        JsonNode payload) {
}
