package com.example.fooddelivery.restaurant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.fooddelivery.events.CancelReason;
import com.example.fooddelivery.events.Envelopes;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.OrderCancelled;
import com.example.fooddelivery.events.OrderPlaced;
import com.example.fooddelivery.events.Topics;
import com.example.fooddelivery.restaurant.support.RestaurantIntegrationTestBase;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** F1: OrderCancelled handled before OrderPlaced leaves a CANCELLED tombstone; the late OrderPlaced is ignored. */
class RestaurantCancelBeforePlaceIT extends RestaurantIntegrationTestBase {

    private String dbStatus(UUID orderId) {
        return jdbc.queryForList("SELECT status FROM kitchen_tickets WHERE order_id = ?", String.class, orderId)
                .stream().findFirst().orElse(null);
    }

    @Test
    void cancelledBeforePlacedKeepsTicketCancelledAndEmitsNoDecision() {
        UUID orderId = UUID.randomUUID();
        OrderPlaced payload = new OrderPlaced(UUID.randomUUID(), PASTA_PLACE,
                List.of(new OrderPlaced.Item(MARGHERITA, 1, new BigDecimal("12.50"))),
                new BigDecimal("12.50"), "5 Customer Ave");
        EventEnvelope placed = Envelopes.first(EventTypes.ORDER_PLACED, orderId, UUID.randomUUID(), payload);
        EventEnvelope cancelled = Envelopes.next(placed, EventTypes.ORDER_CANCELLED,
                new OrderCancelled(CancelReason.TIMEOUT));

        publish(Topics.ORDER_EVENTS, cancelled);
        await().atMost(WAIT).pollInterval(POLL).until(() -> dbStatus(orderId) != null);
        publish(Topics.ORDER_EVENTS, placed);

        await().during(QUIET).atMost(WAIT).pollInterval(POLL).untilAsserted(() -> {
            assertThat(dbStatus(orderId)).isEqualTo("CANCELLED");
            assertThat(eventsFor(Topics.RESTAURANT_EVENTS, orderId)).isEmpty();
        });
        assertThat(jdbc.queryForObject("SELECT count(*) FROM processed_events WHERE event_id = ?", Long.class,
                placed.eventId())).isEqualTo(1L);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM kitchen_tickets WHERE order_id = ?", Long.class,
                orderId)).isEqualTo(1L);
        assertThat(acceptedTickets(PASTA_PLACE)).isZero();
        assertThat(ticketStatus(orderId)).as("tombstone is not exposed by the API").isNull();
    }
}
