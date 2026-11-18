package com.example.fooddelivery.events;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.UUID;

/** JSON encoding and decoding of envelopes and payloads. Every decode problem is an {@link InvalidEventException}. */
public final class EventCodec {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .enable(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES)
            .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE)
            .configure(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES, false)
            .setSerializationInclusion(JsonInclude.Include.ALWAYS);

    private EventCodec() {
    }

    /** Serialises the envelope. {@code causationId} is always written, as null when absent. */
    public static String toJson(EventEnvelope envelope) {
        if (envelope == null) {
            throw new InvalidEventException("envelope is null");
        }
        try {
            return MAPPER.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            throw new InvalidEventException("Cannot serialise envelope: " + e.getOriginalMessage(), e);
        }
    }

    /**
     * Parses and validates an envelope: well-formed JSON, all envelope fields present, known eventType,
     * and a payload that matches the eventType.
     */
    public static EventEnvelope fromJson(String json) {
        if (json == null) {
            throw new InvalidEventException("record value is null");
        }
        JsonNode root;
        try {
            root = MAPPER.readTree(json);
        } catch (JsonProcessingException e) {
            throw new InvalidEventException("Malformed JSON: " + e.getOriginalMessage(), e);
        }
        if (root == null || root.getNodeType() != JsonNodeType.OBJECT) {
            throw new InvalidEventException("Envelope must be a JSON object");
        }
        String eventType = requiredText(root, "eventType");
        if (EventTypes.payloadClass(eventType) == null) {
            throw new InvalidEventException("Unknown eventType: " + eventType);
        }
        JsonNode versionNode = required(root, "eventVersion");
        if (!versionNode.isIntegralNumber() || !versionNode.canConvertToInt()) {
            throw new InvalidEventException("eventVersion must be an integer");
        }
        JsonNode payload = required(root, "payload");
        if (!payload.isObject()) {
            throw new InvalidEventException("payload must be a JSON object");
        }
        JsonNode cause = root.get("causationId");
        UUID causationId = (cause == null || cause.isNull()) ? null : uuid(root, "causationId");
        EventEnvelope envelope = new EventEnvelope(
                uuid(root, "eventId"),
                eventType,
                versionNode.intValue(),
                instant(root, "occurredAt"),
                uuid(root, "orderId"),
                uuid(root, "correlationId"),
                causationId,
                payload);
        payloadOf(envelope);
        return envelope;
    }

    /** Decodes the payload into the record matching the envelope's eventType. */
    public static EventPayload payloadOf(EventEnvelope envelope) {
        if (envelope == null) {
            throw new InvalidEventException("envelope is null");
        }
        Class<? extends EventPayload> type = EventTypes.payloadClass(envelope.eventType());
        if (type == null) {
            throw new InvalidEventException("Unknown eventType: " + envelope.eventType());
        }
        if (envelope.payload() == null || !envelope.payload().isObject()) {
            throw new InvalidEventException("payload must be a JSON object for " + envelope.eventType());
        }
        try {
            return MAPPER.treeToValue(envelope.payload(), type);
        } catch (JsonProcessingException | IllegalArgumentException e) {
            throw new InvalidEventException(
                    "Payload does not match eventType " + envelope.eventType() + ": " + e.getMessage(), e);
        }
    }

    /** Converts a payload record to the JSON tree stored in the envelope. */
    public static JsonNode toNode(EventPayload payload) {
        return MAPPER.valueToTree(payload);
    }

    private static JsonNode required(JsonNode root, String field) {
        JsonNode n = root.get(field);
        if (n == null || n.isNull()) {
            throw new InvalidEventException("Missing envelope field: " + field);
        }
        return n;
    }

    private static String requiredText(JsonNode root, String field) {
        JsonNode n = required(root, field);
        if (!n.isTextual()) {
            throw new InvalidEventException("Envelope field must be a string: " + field);
        }
        return n.asText();
    }

    private static UUID uuid(JsonNode root, String field) {
        String text = requiredText(root, field);
        try {
            return UUID.fromString(text);
        } catch (IllegalArgumentException e) {
            throw new InvalidEventException("Invalid UUID in " + field + ": " + text, e);
        }
    }

    private static Instant instant(JsonNode root, String field) {
        String text = requiredText(root, field);
        try {
            return Instant.parse(text);
        } catch (DateTimeParseException e) {
            throw new InvalidEventException("Invalid ISO-8601 instant in " + field + ": " + text, e);
        }
    }
}
