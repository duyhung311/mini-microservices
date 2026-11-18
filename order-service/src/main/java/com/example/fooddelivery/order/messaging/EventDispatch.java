package com.example.fooddelivery.order.messaging;

import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventHeaders;
import com.example.fooddelivery.events.InvalidEventException;
import com.example.fooddelivery.events.tracing.TraceLinks;
import com.example.fooddelivery.order.domain.OrderService;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;

/** Shared decode-then-apply step and DLT alert of both listeners. */
final class EventDispatch {

    private EventDispatch() {
    }

    /** Decodes and applies. Poison records throw {@link InvalidEventException} (not retried, straight to the DLT). */
    static void dispatch(ConsumerRecord<String, String> record, OrderService orderService) {
        EventEnvelope env = EventCodec.fromJson(record.value());
        checkHeader(record, EventHeaders.EVENT_TYPE, env.eventType());
        checkHeader(record, EventHeaders.CORRELATION_ID, String.valueOf(env.correlationId()));
        try (TraceLinks.Binding ignored = TraceLinks.bindCorrelation(env.correlationId())) {
            orderService.applyEvent(env);
        }
    }

    private static void checkHeader(ConsumerRecord<String, String> record, String name, String expected) {
        Header h = record.headers().lastHeader(name);
        if (h == null) {
            return;
        }
        String actual = EventHeaders.text(h.value());
        if (!expected.equals(actual)) {
            throw new InvalidEventException("Header " + name + "=" + actual + " does not match envelope " + expected);
        }
    }

    static void alertDlt(ConsumerRecord<String, String> record, MeterRegistry registry, Logger log) {
        String eventId = "n/a";
        try {
            eventId = EventCodec.fromJson(record.value()).eventId().toString();
        } catch (RuntimeException ignored) {
            // undecodable poison record
        }
        log.error("ALERT dlt topic={} key={} eventId={}", record.topic(), record.key(), eventId);
        registry.counter("kafka.dlt.records", "topic", record.topic()).increment();
    }
}
