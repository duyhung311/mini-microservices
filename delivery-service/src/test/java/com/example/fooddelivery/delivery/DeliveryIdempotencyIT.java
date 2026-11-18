package com.example.fooddelivery.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.fooddelivery.delivery.support.DeliveryIntegrationTestBase;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.Topics;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Criterion 16: the same record sent twice gives one state change, one outbox row and one downstream event. */
class DeliveryIdempotencyIT extends DeliveryIntegrationTestBase {

    private long processedCount(UUID eventId) {
        Long n = jdbc.queryForObject("SELECT count(*) FROM processed_events WHERE event_id = ?", Long.class, eventId);
        return n == null ? 0 : n;
    }

    private long outboxCount(UUID orderId, String type) {
        Long n = jdbc.queryForObject("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND event_type = ?",
                Long.class, orderId, type);
        return n == null ? 0 : n;
    }

    private long deliveryRows(UUID orderId) {
        Long n = jdbc.queryForObject("SELECT count(*) FROM deliveries WHERE order_id = ?", Long.class, orderId);
        return n == null ? 0 : n;
    }

    @Test
    void sameOrderAcceptedTwiceAssignsOneDriverOnce() {
        UUID orderId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        EventEnvelope accepted = sendAccepted(sendPlaced(orderId, correlationId));
        publish(Topics.RESTAURANT_EVENTS, accepted); // exact duplicate, same eventId

        awaitDeliveryStatus(orderId, "ASSIGNED");
        await().during(QUIET).atMost(WAIT).pollInterval(POLL).until(() ->
                deliveryRows(orderId) == 1
                        && "ASSIGNED".equals(deliveryStatus(orderId))
                        && countDriversWithStatus("BUSY") == 1
                        && outboxCount(orderId, EventTypes.DRIVER_ASSIGNED) == 1
                        && deliveryEvents(orderId, EventTypes.DRIVER_ASSIGNED).size() == 1);

        assertThat(processedCount(accepted.eventId())).isEqualTo(1);
        assertThat(outboxCount(orderId, EventTypes.DELIVERY_FAILED)).isZero();
    }

    @Test
    void duplicateOrderAcceptedIsConsumedAndAbsorbed() {
        UUID orderId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        EventEnvelope accepted = sendAccepted(sendPlaced(orderId, correlationId));
        publish(Topics.RESTAURANT_EVENTS, accepted);
        // same key, same partition, after the duplicate: once it is applied the duplicate was consumed too
        EventEnvelope ready = sendReady(accepted);
        awaitDeliveryStatus(orderId, "READY_FOR_PICKUP");
        await().atMost(WAIT).pollInterval(POLL).until(() -> processedCount(ready.eventId()) == 1);

        assertThat(deliveryRows(orderId)).isEqualTo(1);
        assertThat(countDriversWithStatus("BUSY")).isEqualTo(1);
        assertThat(outboxCount(orderId, EventTypes.DRIVER_ASSIGNED)).isEqualTo(1);
        assertThat(processedCount(accepted.eventId())).isEqualTo(1);
        await().during(QUIET).atMost(WAIT).pollInterval(POLL)
                .until(() -> deliveryEvents(orderId, EventTypes.DRIVER_ASSIGNED).size() == 1);
    }

    @Test
    void sameOrderCancelledTwiceCancelsAndReleasesOnce() {
        UUID orderId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        EventEnvelope[] sent = assignedOrder(orderId, correlationId);
        EventEnvelope cancelled = sendCancelled(sent[0]);
        publish(Topics.ORDER_EVENTS, cancelled); // duplicate
        // marker on the same key and partition after the duplicate
        EventEnvelope marker = sendPlaced(orderId, correlationId);
        await().atMost(WAIT).pollInterval(POLL).until(() -> processedCount(marker.eventId()) == 1);

        assertThat(deliveryStatus(orderId)).isEqualTo("CANCELLED");
        assertThat(deliveryRows(orderId)).isEqualTo(1);
        assertThat(driverStatus(DRIVER_1)).isEqualTo("AVAILABLE");
        assertThat(countDriversWithStatus("BUSY")).isZero();
        assertThat(processedCount(cancelled.eventId())).isEqualTo(1);
        await().during(QUIET).atMost(WAIT).pollInterval(POLL).until(() ->
                outboxCount(orderId, EventTypes.DRIVER_ASSIGNED) == 1
                        && deliveryEvents(orderId, EventTypes.DRIVER_ASSIGNED).size() == 1
                        && "CANCELLED".equals(deliveryStatus(orderId)));
    }

    @Test
    void sameOrderPlacedTwiceKeepsOneDeliveryRow() {
        UUID orderId = UUID.randomUUID();
        EventEnvelope placed = sendPlaced(orderId, UUID.randomUUID());
        publish(Topics.ORDER_EVENTS, placed);
        awaitDeliveryStatus(orderId, "PENDING");
        await().during(QUIET).atMost(WAIT).pollInterval(POLL)
                .until(() -> deliveryRows(orderId) == 1 && processedCount(placed.eventId()) == 1);
    }
}
