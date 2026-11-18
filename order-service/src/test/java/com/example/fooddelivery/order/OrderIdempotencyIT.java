package com.example.fooddelivery.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.events.DeliveryFailureReason;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.Topics;
import java.time.Duration;
import java.util.UUID;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Criterion 16 (order side): the same record sent twice gives one state change, one history row, one outbox row and
 * one downstream record; processed_events holds the eventId once. Also criterion 12: a STALE event is recorded.
 */
class OrderIdempotencyIT extends AbstractOrderIT {

    @Autowired
    private JdbcTemplate jdbc;

    private int processedCount(UUID eventId) {
        return jdbc.queryForObject("select count(*) from processed_events where event_id = ?", Integer.class,
                eventId);
    }

    private int outboxCount(UUID orderId, String type) {
        return jdbc.queryForObject("select count(*) from outbox where aggregate_id = ? and event_type = ?",
                Integer.class, orderId, type);
    }

    @Test
    void duplicateOrderAcceptedAppliesOnce() {
        UUID orderId = UUID.fromString(placeOrder().get("orderId").asText());
        EventEnvelope placed = awaitPlaced(orderId);
        EventEnvelope accepted = SagaEvents.accepted(placed);

        send(Topics.RESTAURANT_EVENTS, accepted);
        send(Topics.RESTAURANT_EVENTS, accepted);

        awaitStatus(orderId, "ACCEPTED");
        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> processedCount(accepted.eventId()) == 1);
        Awaitility.await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(history(orderId)).isEqualTo(historyOf("PLACED", "ACCEPTED"));
            assertThat(processedCount(accepted.eventId())).isEqualTo(1);
            assertThat(status(orderId)).isEqualTo("ACCEPTED");
        });
        assertThat(outboxCount(orderId, "OrderPlaced")).isEqualTo(1);
    }

    @Test
    void duplicateDeliveryFailedCancelsOnceAndEmitsOneOrderCancelled() {
        UUID orderId = UUID.fromString(placeOrder().get("orderId").asText());
        EventEnvelope placed = awaitPlaced(orderId);
        EventEnvelope accepted = SagaEvents.accepted(placed);
        send(Topics.RESTAURANT_EVENTS, accepted);
        awaitStatus(orderId, "ACCEPTED");

        EventEnvelope failed = SagaEvents.deliveryFailed(accepted, DeliveryFailureReason.NO_DRIVER_AVAILABLE);
        send(Topics.DELIVERY_EVENTS, failed);
        send(Topics.DELIVERY_EVENTS, failed);

        awaitStatus(orderId, "CANCELLED");
        awaitOrderEvents(orderId, "OrderCancelled", 1);

        Awaitility.await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(history(orderId)).isEqualTo(historyOf("PLACED", "ACCEPTED", "CANCELLED"));
            assertThat(processedCount(failed.eventId())).isEqualTo(1);
            assertThat(outboxCount(orderId, "OrderCancelled")).isEqualTo(1);
            assertThat(orderEventsOfType(orderId, "OrderCancelled")).hasSize(1);
        });
    }

    @Test
    void staleEventIsRecordedInProcessedEventsOnceEvenWhenDuplicated() {
        UUID orderId = UUID.fromString(placeOrder().get("orderId").asText());
        EventEnvelope placed = awaitPlaced(orderId);
        EventEnvelope accepted = SagaEvents.accepted(placed);
        send(Topics.RESTAURANT_EVENTS, accepted);
        awaitStatus(orderId, "ACCEPTED");
        assertThat(post("/orders/" + orderId + "/cancel").getStatusCode().value()).isEqualTo(200);
        awaitStatus(orderId, "CANCELLED");
        java.util.List<String> before = history(orderId);

        EventEnvelope staleReady = SagaEvents.ready(placed);
        send(Topics.RESTAURANT_EVENTS, staleReady);
        send(Topics.RESTAURANT_EVENTS, staleReady);

        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> processedCount(staleReady.eventId()) == 1);
        Awaitility.await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(processedCount(staleReady.eventId())).isEqualTo(1);
            assertThat(status(orderId)).isEqualTo("CANCELLED");
            assertThat(history(orderId)).isEqualTo(before);
        });
    }
}
