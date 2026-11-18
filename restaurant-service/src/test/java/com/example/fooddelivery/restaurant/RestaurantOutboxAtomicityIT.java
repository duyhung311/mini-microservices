package com.example.fooddelivery.restaurant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.example.fooddelivery.events.CancelReason;
import com.example.fooddelivery.events.Envelopes;
import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventHeaders;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.OrderCancelled;
import com.example.fooddelivery.events.Topics;
import com.example.fooddelivery.restaurant.outbox.OutboxEventPublisher;
import com.example.fooddelivery.restaurant.support.LiveContexts;
import com.example.fooddelivery.restaurant.support.RestaurantFaults;
import com.example.fooddelivery.restaurant.support.RestaurantIntegrationTestBase;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.test.context.TestPropertySource;

/**
 * Criterion 14: the business change and its outbox row commit or roll back together. The outbox insert is made to
 * fail with a database trigger (shared by every cached Spring context), once immediately and once at commit time.
 */
@TestPropertySource(properties = {
        "app.kafka.retry.delay-ms=200", "app.kafka.retry.multiplier=2", "app.kafka.retry.max-delay-ms=1000"})
class RestaurantOutboxAtomicityIT extends RestaurantIntegrationTestBase {

    private static List<KafkaListenerEndpointRegistry> silenced;

    @Autowired
    private ApplicationContext context;

    /** Cached contexts of earlier classes share the consumer group; silence them so this context handles the records. */
    @BeforeEach
    void silenceOtherContexts() {
        if (silenced == null) {
            silenced = LiveContexts.stopListenersExcept(context);
        }
    }

    @AfterAll
    static void restoreOtherContexts() {
        if (silenced != null) {
            LiveContexts.restart(silenced);
            silenced = null;
        }
    }

    @Autowired
    private OutboxEventPublisher outboxPublisher;

    @Autowired
    private MeterRegistry meters;

    private long count(String sql, Object... args) {
        Long n = jdbc.queryForObject(sql, Long.class, args);
        return n == null ? 0 : n;
    }

    private void assertNothingCommitted(EventEnvelope placed) {
        await().during(QUIET).atMost(WAIT).pollInterval(POLL).untilAsserted(() -> {
            assertThat(count("SELECT count(*) FROM kitchen_tickets WHERE order_id = ?", placed.orderId())).isZero();
            assertThat(count("SELECT count(*) FROM outbox WHERE aggregate_id = ?", placed.orderId())).isZero();
            assertThat(count("SELECT count(*) FROM processed_events WHERE event_id = ?", placed.eventId())).isZero();
            assertThat(ticketStatus(placed.orderId())).isNull();
        });
    }

    @Test
    void failingOutboxInsertRollsBackTheTicketAndTheProcessedMarker() {
        EventEnvelope placed = RestaurantFaults.orderPlaced(PASTA_PLACE, MARGHERITA);
        try (RestaurantFaults.Fault fault = RestaurantFaults.failOutboxInsert(jdbc, placed.orderId())) {
            publish(Topics.ORDER_EVENTS, placed);

            await().atMost(WAIT).pollInterval(POLL).until(() -> fault.attempts() >= 1);
            assertNothingCommitted(placed);
            // Keep the fault until the retries are exhausted, so no late retry commits after the trigger is gone.
            awaitEvents(Topics.dlt(Topics.ORDER_EVENTS, Topics.RESTAURANT_SERVICE), placed.orderId(),
                    EventTypes.ORDER_PLACED);
            assertNothingCommitted(placed);
        }
    }

    @Test
    void outboxInsertFailingAtCommitRollsBackTheAlreadyWrittenTicket() {
        EventEnvelope placed = RestaurantFaults.orderPlaced(PASTA_PLACE, MARGHERITA);
        try (RestaurantFaults.Fault fault = RestaurantFaults.failOutboxAtCommit(jdbc, placed.orderId())) {
            publish(Topics.ORDER_EVENTS, placed);

            await().atMost(WAIT).pollInterval(POLL).until(() -> fault.attempts() >= 1);
            assertNothingCommitted(placed);
            // Keep the fault until the retries are exhausted, so no late retry commits after the trigger is gone.
            awaitEvents(Topics.dlt(Topics.ORDER_EVENTS, Topics.RESTAURANT_SERVICE), placed.orderId(),
                    EventTypes.ORDER_PLACED);
            assertNothingCommitted(placed);
        }
    }

    @Test
    void successCommitsTicketAndOutboxRowTogetherAndRelayPublishesIt() {
        EventEnvelope placed = placeOrder(PASTA_PLACE, MARGHERITA);
        awaitTicketStatus(placed.orderId(), "ACCEPTED");

        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT id, event_type, payload::text AS payload FROM outbox WHERE aggregate_id = ?",
                placed.orderId());
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get("event_type")).isEqualTo(EventTypes.ORDER_ACCEPTED);
        EventEnvelope stored = EventCodec.fromJson((String) rows.get(0).get("payload"));
        assertThat(stored.orderId()).isEqualTo(placed.orderId());
        assertThat(stored.causationId()).isEqualTo(placed.eventId());
        assertThat(stored.correlationId()).isEqualTo(placed.correlationId());

        await().atMost(WAIT).pollInterval(POLL).untilAsserted(() -> assertThat(count(
                "SELECT count(*) FROM outbox WHERE aggregate_id = ? AND published_at IS NOT NULL",
                placed.orderId())).isEqualTo(1));

        List<Published> published = awaitEvents(Topics.RESTAURANT_EVENTS, placed.orderId(), EventTypes.ORDER_ACCEPTED);
        assertThat(published).hasSize(1);
        Published record = published.get(0);
        assertThat(record.key()).isEqualTo(placed.orderId().toString());
        assertThat(record.envelope()).isEqualTo(stored);
        assertThat(record.headers()).containsEntry(EventHeaders.EVENT_TYPE, EventTypes.ORDER_ACCEPTED)
                .containsEntry(EventHeaders.EVENT_VERSION, "1")
                .containsEntry(EventHeaders.CORRELATION_ID, placed.correlationId().toString());
    }

    @Test
    void rejectionCommitsOutboxRowWithoutTicket() {
        EventEnvelope placed = placeOrder(NIGHT_GRILL, MARGHERITA);
        awaitEvents(Topics.RESTAURANT_EVENTS, placed.orderId(), EventTypes.ORDER_REJECTED);

        assertThat(count("SELECT count(*) FROM kitchen_tickets WHERE order_id = ?", placed.orderId())).isZero();
        assertThat(count("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND event_type = ?",
                placed.orderId(), EventTypes.ORDER_REJECTED)).isEqualTo(1);
    }

    @Test
    void outboxPendingGaugeIsRegistered() {
        assertThat(meters.find("outbox.pending").gauge()).isNotNull();
    }

    @Test
    void outboxPublisherRefusesToRunWithoutATransaction() {
        EventEnvelope orphan = Envelopes.first(EventTypes.ORDER_CANCELLED, UUID.randomUUID(), UUID.randomUUID(),
                new OrderCancelled(CancelReason.TIMEOUT));
        assertThatThrownBy(() -> outboxPublisher.publish(orphan)).isInstanceOf(RuntimeException.class);
        assertThat(count("SELECT count(*) FROM outbox WHERE aggregate_id = ?", orphan.orderId())).isZero();
    }
}
