package com.example.fooddelivery.restaurant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.fooddelivery.events.CancelReason;
import com.example.fooddelivery.events.DeliveryFailed;
import com.example.fooddelivery.events.DeliveryFailureReason;
import com.example.fooddelivery.events.Envelopes;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.OrderCancelled;
import com.example.fooddelivery.events.Topics;
import com.example.fooddelivery.restaurant.idempotency.ProcessedEvents;
import com.example.fooddelivery.restaurant.support.RestaurantIntegrationTestBase;
import com.example.fooddelivery.restaurant.support.RestaurantKafkaRaw;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

/** Criterion 16: the same record delivered twice gives one state change, one outbox row, one downstream event. */
class RestaurantIdempotencyIT extends RestaurantIntegrationTestBase {

    @Autowired
    private ProcessedEvents processedEvents;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrap;

    private long count(String sql, Object... args) {
        Long n = jdbc.queryForObject(sql, Long.class, args);
        return n == null ? 0 : n;
    }

    private long outboxRows(UUID orderId, String type) {
        return count("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND event_type = ?", orderId, type);
    }

    private long processed(UUID eventId) {
        return count("SELECT count(*) FROM processed_events WHERE event_id = ?", eventId);
    }

    private long tickets(UUID orderId) {
        return count("SELECT count(*) FROM kitchen_tickets WHERE order_id = ?", orderId);
    }

    /** Waits until the group has committed every record on the topic, i.e. the duplicate was really consumed. */
    private void awaitConsumed(String topic) {
        await().atMost(WAIT).pollInterval(POLL)
                .until(() -> RestaurantKafkaRaw.groupLag(bootstrap, topic, Topics.RESTAURANT_SERVICE) == 0);
    }

    @Test
    void duplicateOrderPlacedGivesOneTicketOneOutboxRowOneDownstreamRecord() {
        EventEnvelope placed = placeOrder(PASTA_PLACE, MARGHERITA);
        awaitTicketStatus(placed.orderId(), "ACCEPTED");
        publish(Topics.ORDER_EVENTS, placed);
        awaitConsumed(Topics.ORDER_EVENTS);

        await().during(QUIET).atMost(WAIT).pollInterval(POLL).untilAsserted(() -> {
            assertThat(tickets(placed.orderId())).isEqualTo(1);
            assertThat(outboxRows(placed.orderId(), EventTypes.ORDER_ACCEPTED)).isEqualTo(1);
            assertThat(outboxRows(placed.orderId(), EventTypes.ORDER_REJECTED)).isZero();
            assertThat(processed(placed.eventId())).isEqualTo(1);
            assertThat(eventsFor(Topics.RESTAURANT_EVENTS, placed.orderId())).hasSize(1);
            assertThat(ticketStatus(placed.orderId())).isEqualTo("ACCEPTED");
        });
        assertThat(eventsOfType(Topics.RESTAURANT_EVENTS, placed.orderId(), EventTypes.ORDER_ACCEPTED)).hasSize(1);
    }

    @Test
    void duplicateOrderPlacedForClosedRestaurantGivesOneRejection() {
        EventEnvelope placed = placeOrder(NIGHT_GRILL, MARGHERITA);
        awaitEvents(Topics.RESTAURANT_EVENTS, placed.orderId(), EventTypes.ORDER_REJECTED);
        publish(Topics.ORDER_EVENTS, placed);
        awaitConsumed(Topics.ORDER_EVENTS);

        await().during(QUIET).atMost(WAIT).pollInterval(POLL).untilAsserted(() -> {
            assertThat(tickets(placed.orderId())).isZero();
            assertThat(outboxRows(placed.orderId(), EventTypes.ORDER_REJECTED)).isEqualTo(1);
            assertThat(processed(placed.eventId())).isEqualTo(1);
            assertThat(eventsFor(Topics.RESTAURANT_EVENTS, placed.orderId())).hasSize(1);
        });
    }

    @Test
    void duplicateDeliveryFailedIsOneTransitionAndOneProcessedRow() {
        EventEnvelope placed = placeOrder(PASTA_PLACE, MARGHERITA);
        awaitTicketStatus(placed.orderId(), "ACCEPTED");
        EventEnvelope failed = Envelopes.next(placed, EventTypes.DELIVERY_FAILED,
                new DeliveryFailed(DeliveryFailureReason.NO_DRIVER_AVAILABLE));

        publish(Topics.DELIVERY_EVENTS, failed);
        awaitTicketStatus(placed.orderId(), "ABANDONED");
        publish(Topics.DELIVERY_EVENTS, failed);
        awaitConsumed(Topics.DELIVERY_EVENTS);

        await().during(QUIET).atMost(WAIT).pollInterval(POLL).untilAsserted(() -> {
            assertThat(ticketStatus(placed.orderId())).isEqualTo("ABANDONED");
            assertThat(processed(failed.eventId())).isEqualTo(1);
            assertThat(tickets(placed.orderId())).isEqualTo(1);
            assertThat(eventsFor(Topics.RESTAURANT_EVENTS, placed.orderId())).hasSize(1);
            assertThat(outboxRows(placed.orderId(), EventTypes.ORDER_ACCEPTED)).isEqualTo(1);
        });
    }

    @Test
    void duplicateOrderCancelledIsOneTransitionAndOneProcessedRow() {
        EventEnvelope placed = placeOrder(PASTA_PLACE, MARGHERITA);
        awaitTicketStatus(placed.orderId(), "ACCEPTED");
        EventEnvelope cancelled = Envelopes.next(placed, EventTypes.ORDER_CANCELLED,
                new OrderCancelled(CancelReason.CUSTOMER_REQUEST));

        publish(Topics.ORDER_EVENTS, cancelled);
        awaitTicketStatus(placed.orderId(), "CANCELLED");
        publish(Topics.ORDER_EVENTS, cancelled);
        awaitConsumed(Topics.ORDER_EVENTS);

        await().during(QUIET).atMost(WAIT).pollInterval(POLL).untilAsserted(() -> {
            assertThat(ticketStatus(placed.orderId())).isEqualTo("CANCELLED");
            assertThat(processed(cancelled.eventId())).isEqualTo(1);
            assertThat(eventsFor(Topics.RESTAURANT_EVENTS, placed.orderId())).hasSize(1);
        });
    }

    @Test
    void firstCallOfTheGuardReturnsTrueAndTheSecondReturnsFalse() {
        UUID eventId = UUID.randomUUID();
        assertThat(processedEvents.markProcessed(eventId)).isTrue();
        assertThat(processedEvents.markProcessed(eventId)).isFalse();
        assertThat(processed(eventId)).isEqualTo(1);
    }
}
