package com.example.fooddelivery.delivery.outbox;

import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventHeaders;
import com.example.fooddelivery.events.Topics;
import com.example.fooddelivery.events.tracing.TraceLinks;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.tracing.Tracer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/** Polls the outbox and publishes unpublished rows to delivery.events in id order. */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int BATCH = 100;

    private final JdbcTemplate jdbc;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectProvider<Tracer> tracer;

    public OutboxRelay(JdbcTemplate jdbc, KafkaTemplate<String, String> kafkaTemplate, MeterRegistry registry,
            ObjectProvider<Tracer> tracer) {
        this.tracer = tracer;
        this.jdbc = jdbc;
        this.kafkaTemplate = kafkaTemplate;
        Gauge.builder("outbox.pending", this, OutboxRelay::pending).register(registry);
    }

    double pending() {
        try {
            Long n = jdbc.queryForObject("SELECT count(*) FROM outbox WHERE published_at IS NULL", Long.class);
            return n == null ? 0 : n;
        } catch (RuntimeException e) {
            return Double.NaN;
        }
    }

    @Scheduled(fixedDelayString = "${outbox.relay.poll-ms:500}")
    @Transactional
    public void relay() {
        Boolean locked = jdbc.queryForObject(
                "SELECT pg_try_advisory_xact_lock(hashtext('delivery-service-outbox'))", Boolean.class);
        if (!Boolean.TRUE.equals(locked)) {
            return;
        }
        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT id, aggregate_id, payload::text AS payload, traceparent FROM outbox WHERE published_at IS NULL "
                        + "ORDER BY id LIMIT " + BATCH + " FOR UPDATE");
        for (Map<String, Object> row : rows) {
            long id = ((Number) row.get("id")).longValue();
            try {
                EventEnvelope env = EventCodec.fromJson((String) row.get("payload"));
                // jsonb reformats the text; re-serialize so the published value is the canonical compact JSON
                String payload = EventCodec.toJson(env);
                ProducerRecord<String, String> record = new ProducerRecord<>(Topics.DELIVERY_EVENTS, null,
                        row.get("aggregate_id").toString(), payload);
                EventHeaders.from(env).forEach((k, v) -> record.headers().add(k, v));
                TraceLinks.inChildSpan(tracer.getIfAvailable(), (String) row.get("traceparent"),
                        "outbox relay publish", env.correlationId(), env.eventType(), id,
                        () -> kafkaTemplate.send(record).get(10, TimeUnit.SECONDS));
                jdbc.update("UPDATE outbox SET published_at = now() WHERE id = ?", id);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Outbox relay interrupted at row {}", id);
                return;
            } catch (Exception e) {
                log.warn("Outbox relay stopped at row {}: {}", id, e.toString());
                return;
            }
        }
    }
}
