package com.example.fooddelivery.events;

import java.time.Instant;
import java.util.UUID;

/** Factory methods that keep eventId, version, correlation and causation rules in one place. */
public final class Envelopes {

    public static final int CURRENT_VERSION = 1;

    private Envelopes() {
    }

    /**
     * First event of a saga step that is not caused by another event (REST or scheduler origin):
     * causationId is null.
     */
    public static EventEnvelope first(String type, UUID orderId, UUID correlationId, EventPayload payload) {
        return build(type, orderId, correlationId, null, payload);
    }

    /** Event caused by {@code cause}: copies orderId and correlationId, sets causationId = cause.eventId. */
    public static EventEnvelope next(EventEnvelope cause, String type, EventPayload payload) {
        return build(type, cause.orderId(), cause.correlationId(), cause.eventId(), payload);
    }

    private static EventEnvelope build(String type, UUID orderId, UUID correlationId, UUID causationId,
                                       EventPayload payload) {
        Class<? extends EventPayload> expected = EventTypes.payloadClass(type);
        if (expected == null) {
            throw new IllegalArgumentException("Unknown eventType: " + type);
        }
        if (!expected.isInstance(payload)) {
            throw new IllegalArgumentException("Payload "
                    + (payload == null ? "null" : payload.getClass().getSimpleName())
                    + " does not match eventType " + type);
        }
        return new EventEnvelope(UUID.randomUUID(), type, CURRENT_VERSION, Instant.now(), orderId,
                correlationId, causationId, EventCodec.toNode(payload));
    }
}
