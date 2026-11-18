package com.example.fooddelivery.restaurant.outbox;

import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.tracing.TraceLinks;
import io.micrometer.tracing.Tracer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Writes the envelope to the outbox table in the caller's transaction; {@link OutboxRelay} sends it to Kafka.
 * The row also stores a W3C traceparent inside the saga trace (traceId == correlationId, see {@link TraceLinks}),
 * so the relay can continue the same trace asynchronously.
 */
@Component
public class OutboxEventPublisher implements EventPublisher {

    private final JdbcTemplate jdbc;
    private final ObjectProvider<Tracer> tracer;

    public OutboxEventPublisher(JdbcTemplate jdbc, ObjectProvider<Tracer> tracer) {
        this.jdbc = jdbc;
        this.tracer = tracer;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void publish(EventEnvelope envelope) {
        String traceparent = TraceLinks.capture(tracer.getIfAvailable(), envelope.correlationId(),
                envelope.eventType());
        jdbc.update("INSERT INTO outbox(aggregate_id, event_type, payload, traceparent) "
                        + "VALUES (?, ?, CAST(? AS jsonb), ?)",
                envelope.orderId(), envelope.eventType(), EventCodec.toJson(envelope), traceparent);
    }
}
