package com.example.fooddelivery.restaurant.messaging;

import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventHeaders;
import com.example.fooddelivery.events.InvalidEventException;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

/** Decodes and validates a record; every problem is an InvalidEventException (poison, straight to the DLT). */
final class EventDecoding {

    private EventDecoding() {
    }

    static EventEnvelope decode(ConsumerRecord<String, String> record) {
        EventEnvelope envelope = EventCodec.fromJson(record.value());
        // Validates the payload against the event type (throws InvalidEventException on mismatch).
        EventCodec.payloadOf(envelope);
        if (envelope.eventVersion() != 1) {
            throw new InvalidEventException("Unsupported eventVersion " + envelope.eventVersion());
        }
        checkHeader(record, EventHeaders.EVENT_TYPE, envelope.eventType());
        checkHeader(record, EventHeaders.EVENT_VERSION, String.valueOf(envelope.eventVersion()));
        checkHeader(record, EventHeaders.CORRELATION_ID, String.valueOf(envelope.correlationId()));
        return envelope;
    }

    private static void checkHeader(ConsumerRecord<String, String> record, String name, String expected) {
        Header header = record.headers().lastHeader(name);
        if (header == null) {
            throw new InvalidEventException("Missing header " + name);
        }
        if (!Objects.equals(EventHeaders.text(header.value()), expected)) {
            throw new InvalidEventException("Header " + name + "=" + EventHeaders.text(header.value())
                    + " does not match envelope value " + expected);
        }
    }

    /** Best-effort event id for alert logs. */
    static String eventIdOrNa(String json) {
        try {
            return String.valueOf(EventCodec.fromJson(json).eventId());
        } catch (RuntimeException e) {
            return "n/a";
        }
    }
}
