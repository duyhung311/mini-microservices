package com.example.fooddelivery.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.fooddelivery.events.CancelReason;
import com.example.fooddelivery.events.DeliveryFailureReason;
import com.example.fooddelivery.events.Envelopes;
import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.OrderCancelled;
import com.example.fooddelivery.events.Topics;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.IllegalTransactionStateException;
import com.example.fooddelivery.order.outbox.EventPublisher;

/**
 * Criterion 14 (order side): the business change and its outbox row commit or roll back together. The outbox insert is
 * forced to fail with a database trigger that is scoped to one address or one order, so it works in the shared context.
 */
class OrderOutboxAtomicityIT extends AbstractOrderIT {

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private EventPublisher publisher;

    private int count(String sql, Object... args) {
        return jdbc.queryForObject(sql, Integer.class, args);
    }

    @Test
    void successPathCommitsBusinessRowAndOutboxRowTogetherAndRelayPublishesThem() {
        JsonNode body = placeOrder();
        UUID orderId = UUID.fromString(body.get("orderId").asText());
        UUID correlationId = UUID.fromString(body.get("correlationId").asText());

        // committed together by the time the 202 is returned
        assertThat(count("select count(*) from orders where id = ?", orderId)).isEqualTo(1);
        assertThat(count("select count(*) from order_status_history where order_id = ?", orderId)).isEqualTo(1);
        assertThat(count("select count(*) from outbox where aggregate_id = ? and event_type = 'OrderPlaced'",
                orderId)).isEqualTo(1);

        // relay publishes: published_at set, key = aggregate id, headers equal the envelope
        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> count(
                "select count(*) from outbox where aggregate_id = ? and published_at is not null", orderId) == 1);
        List<Received> records = awaitOrderEvents(orderId, "OrderPlaced", 1);
        Received r = records.get(0);
        assertThat(r.key()).isEqualTo(orderId.toString());
        assertThat(r.headers()).containsEntry("eventType", "OrderPlaced").containsEntry("eventVersion", "1")
                .containsEntry("correlationId", correlationId.toString());
        assertThat(r.envelope().orderId()).isEqualTo(orderId);
        assertThat(r.envelope().correlationId()).isEqualTo(correlationId);
        // the history row carries the eventId of the published envelope
        UUID historyEventId = jdbc.queryForObject(
                "select event_id from order_status_history where order_id = ?", UUID.class, orderId);
        assertThat(r.envelope().eventId()).isEqualTo(historyEventId);
        // the relay publishes the canonical compact envelope JSON (not the spaced jsonb text)
        assertThat(r.raw()).isEqualTo(EventCodec.toJson(EventCodec.fromJson(r.raw())));
    }

    @Test
    void failingOutboxInsertRollsBackTheNewOrder() {
        String marker = "ATOMICITY-" + UUID.randomUUID();
        int orders = count("select count(*) from orders");
        int items = count("select count(*) from order_items");
        int outbox = count("select count(*) from outbox");
        String json = orderJson(UUID.randomUUID(), UUID.randomUUID(),
                "[" + itemJson(UUID.randomUUID(), 1, "9.99") + "]", marker);

        try (DbFaults.Fault ignored = DbFaults.failOutboxInsertForAddress(jdbc, marker)) {
            HttpHeaders h = new HttpHeaders();
            h.setContentType(MediaType.APPLICATION_JSON);
            ResponseEntity<String> r = rest.exchange("/orders", HttpMethod.POST, new HttpEntity<>(json, h),
                    String.class);
            assertThat(r.getStatusCode().is2xxSuccessful()).as("status %s", r.getStatusCode()).isFalse();
        }

        assertThat(count("select count(*) from orders where delivery_address = ?", marker)).isZero();
        assertThat(count("select count(*) from orders")).isEqualTo(orders);
        assertThat(count("select count(*) from order_items")).isEqualTo(items);
        assertThat(count("select count(*) from outbox")).isEqualTo(outbox);
    }

    @Test
    void failingOutboxInsertRollsBackTheStateChangeHistoryAndProcessedEventOfAConsumedEvent() {
        UUID orderId = UUID.fromString(placeOrder().get("orderId").asText());
        EventEnvelope placed = awaitPlaced(orderId);
        EventEnvelope accepted = SagaEvents.accepted(placed);
        send(Topics.RESTAURANT_EVENTS, accepted);
        awaitStatus(orderId, "ACCEPTED");
        List<String> before = history(orderId);

        EventEnvelope failed = SagaEvents.deliveryFailed(accepted, DeliveryFailureReason.NO_DRIVER_AVAILABLE);
        TopicTap tap = TopicTap.instance();
        String dlt = Topics.dlt(Topics.DELIVERY_EVENTS, Topics.ORDER_SERVICE);
        try (DbFaults.Fault ignored = DbFaults.failOutboxInsertFor(jdbc, orderId, "OrderCancelled")) {
            send(Topics.DELIVERY_EVENTS, failed);
            // every attempt rolls back, so the record is dead-lettered after the retries
            Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> tap.count(dlt, orderId) >= 1);
        }

        assertThat(status(orderId)).isEqualTo("ACCEPTED");
        assertThat(history(orderId)).isEqualTo(before);
        assertThat(count("select count(*) from outbox where aggregate_id = ? and event_type = 'OrderCancelled'",
                orderId)).isZero();
        assertThat(count("select count(*) from processed_events where event_id = ?", failed.eventId())).isZero();
    }

    @Test
    void outboxPublisherRequiresAnExistingTransaction() {
        EventEnvelope env = Envelopes.first(EventTypes.ORDER_CANCELLED, UUID.randomUUID(), UUID.randomUUID(),
                new OrderCancelled(CancelReason.CUSTOMER_REQUEST));
        assertThatThrownBy(() -> publisher.publish(env)).isInstanceOf(IllegalTransactionStateException.class);
        assertThat(count("select count(*) from outbox where aggregate_id = ?", env.orderId())).isZero();
    }
}
