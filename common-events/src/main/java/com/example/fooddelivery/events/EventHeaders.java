package com.example.fooddelivery.events;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/** Kafka header names and a helper that builds their UTF-8 values from an envelope. */
public final class EventHeaders {

    public static final String EVENT_TYPE = "eventType";
    public static final String EVENT_VERSION = "eventVersion";
    public static final String CORRELATION_ID = "correlationId";

    private EventHeaders() {
    }

    /** Header name to UTF-8 value, in a stable order. */
    public static Map<String, byte[]> from(EventEnvelope envelope) {
        Map<String, byte[]> headers = new LinkedHashMap<>();
        headers.put(EVENT_TYPE, utf8(envelope.eventType()));
        headers.put(EVENT_VERSION, utf8(String.valueOf(envelope.eventVersion())));
        headers.put(CORRELATION_ID, utf8(String.valueOf(envelope.correlationId())));
        return headers;
    }

    public static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public static String text(byte[] value) {
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }
}
