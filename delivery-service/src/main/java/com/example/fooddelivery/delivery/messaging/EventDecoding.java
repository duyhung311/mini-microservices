package com.example.fooddelivery.delivery.messaging;

import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventHeaders;
import com.example.fooddelivery.events.InvalidEventException;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;

/** Decoding and DLT alert helpers shared by the listeners. */
final class EventDecoding {

    private EventDecoding() {
    }

    /** Decode and validate; throws {@link InvalidEventException} (not retried) on any problem. */
    static EventEnvelope decode(ConsumerRecord<String, String> record) {
        EventEnvelope env = EventCodec.fromJson(record.value());
        EventCodec.payloadOf(env);
        if (env.eventVersion() != 1) {
            throw new InvalidEventException("unsupported eventVersion " + env.eventVersion());
        }
        String type = requiredHeader(record, EventHeaders.EVENT_TYPE);
        if (!type.equals(env.eventType())) {
            throw new InvalidEventException("eventType header " + type + " does not match envelope " + env.eventType());
        }
        String version = requiredHeader(record, EventHeaders.EVENT_VERSION);
        if (!version.equals(String.valueOf(env.eventVersion()))) {
            throw new InvalidEventException("eventVersion header " + version + " does not match envelope");
        }
        String corr = requiredHeader(record, EventHeaders.CORRELATION_ID);
        if (!corr.equals(String.valueOf(env.correlationId()))) {
            throw new InvalidEventException("correlationId header does not match envelope");
        }
        return env;
    }

    static void alert(Logger log, MeterRegistry registry, ConsumerRecord<String, String> record) {
        String eventId = "n/a";
        try {
            eventId = String.valueOf(EventCodec.fromJson(record.value()).eventId());
        } catch (RuntimeException ignored) {
            // undecodable record: eventId stays n/a
        }
        log.error("ALERT dlt topic={} key={} eventId={}", record.topic(), record.key(), eventId);
        registry.counter("kafka.dlt.records", "topic", record.topic()).increment();
    }

    private static String requiredHeader(ConsumerRecord<String, String> record, String name) {
        String value = header(record, name);
        if (value == null) {
            throw new InvalidEventException("missing header " + name);
        }
        return value;
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null || h.value() == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
