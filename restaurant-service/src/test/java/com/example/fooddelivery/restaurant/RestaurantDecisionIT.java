package com.example.fooddelivery.restaurant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.OrderAccepted;
import com.example.fooddelivery.events.OrderRejected;
import com.example.fooddelivery.events.RejectReason;
import com.example.fooddelivery.events.Topics;
import com.example.fooddelivery.restaurant.support.RestaurantIntegrationTestBase;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Criteria 3 (key = orderId), 4 (headers, causation) and 7 (three rejection reasons), restaurant side. */
class RestaurantDecisionIT extends RestaurantIntegrationTestBase {

    private void assertEnvelopeMatchesRecord(Published record, EventEnvelope placed, String expectedType) {
        EventEnvelope out = record.envelope();
        assertThat(record.key()).isEqualTo(placed.orderId().toString());
        assertThat(out.orderId()).isEqualTo(placed.orderId());
        assertThat(out.eventType()).isEqualTo(expectedType);
        assertThat(out.eventVersion()).isEqualTo(1);
        assertThat(out.causationId()).isEqualTo(placed.eventId());
        assertThat(out.correlationId()).isEqualTo(placed.correlationId());
        assertThat(out.eventId()).isNotEqualTo(placed.eventId());
        assertThat(out.occurredAt()).isNotNull();
        assertThat(record.headers().get("eventType")).isEqualTo(out.eventType());
        assertThat(record.headers().get("eventVersion")).isEqualTo(String.valueOf(out.eventVersion()));
        assertThat(record.headers().get("correlationId")).isEqualTo(out.correlationId().toString());
    }

    private void assertRejected(EventEnvelope placed, UUID restaurantId, RejectReason reason) {
        List<Published> rejected = awaitEvents(Topics.RESTAURANT_EVENTS, placed.orderId(),
                EventTypes.ORDER_REJECTED);
        Published record = rejected.get(0);

        assertEnvelopeMatchesRecord(record, placed, EventTypes.ORDER_REJECTED);
        OrderRejected payload = (OrderRejected) EventCodec.payloadOf(record.envelope());
        assertThat(payload.reason()).isEqualTo(reason);
        assertThat(payload.restaurantId()).isEqualTo(restaurantId);

        // exactly one record for the order and no OrderAccepted, no ticket
        await().during(QUIET).atMost(WAIT).pollInterval(POLL).until(() -> {
            List<Published> all = eventsFor(Topics.RESTAURANT_EVENTS, placed.orderId());
            return all.size() == 1 && all.get(0).type().equals(EventTypes.ORDER_REJECTED);
        });
        assertThat(ticketStatus(placed.orderId())).isNull();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM kitchen_tickets WHERE order_id = ?", Long.class,
                placed.orderId())).isZero();
    }

    @Test
    void acceptsOrderPublishesExactlyOneOrderAcceptedAndCreatesAcceptedTicket() {
        EventEnvelope placed = placeOrder(PASTA_PLACE, MARGHERITA);

        List<Published> accepted = awaitEvents(Topics.RESTAURANT_EVENTS, placed.orderId(),
                EventTypes.ORDER_ACCEPTED);
        Published record = accepted.get(0);

        assertEnvelopeMatchesRecord(record, placed, EventTypes.ORDER_ACCEPTED);
        OrderAccepted payload = (OrderAccepted) EventCodec.payloadOf(record.envelope());
        assertThat(payload.restaurantId()).isEqualTo(PASTA_PLACE);
        assertThat(payload.estimatedPrepMinutes()).isEqualTo(15);
        assertThat(payload.pickupAddress()).isEqualTo("1 Kitchen St");

        awaitTicketStatus(placed.orderId(), "ACCEPTED");

        // exactly one event for this order on restaurant.events, all on one partition
        await().during(QUIET).atMost(WAIT).pollInterval(POLL).until(() -> {
            List<Published> all = eventsFor(Topics.RESTAURANT_EVENTS, placed.orderId());
            return all.size() == 1 && all.get(0).type().equals(EventTypes.ORDER_ACCEPTED);
        });
        assertThat(jdbc.queryForObject("SELECT count(*) FROM kitchen_tickets WHERE order_id = ?", Long.class,
                placed.orderId())).isEqualTo(1L);
    }

    @Test
    void closedRestaurantRejectsWithClosed() {
        // Night Grill is closed and Margherita is not on its menu: CLOSED must win over ITEM_UNAVAILABLE
        EventEnvelope placed = placeOrder(NIGHT_GRILL, MARGHERITA);

        assertRejected(placed, NIGHT_GRILL, RejectReason.CLOSED);
    }

    @Test
    void restaurantClosedViaPatchRejectsWithClosed() {
        try {
            assertThat(patchRestaurant(PASTA_PLACE, "{\"open\":false}").status()).isEqualTo(200);
            EventEnvelope placed = placeOrder(PASTA_PLACE, MARGHERITA);

            assertRejected(placed, PASTA_PLACE, RejectReason.CLOSED);
        } finally {
            patchRestaurant(PASTA_PLACE, "{\"open\":true}");
        }
    }

    @Test
    void unknownRestaurantRejectsWithClosed() {
        UUID unknown = UUID.randomUUID();
        EventEnvelope placed = placeOrder(unknown, MARGHERITA);

        assertRejected(placed, unknown, RejectReason.CLOSED);
    }

    @Test
    void unavailableItemRejectsWithItemUnavailable() {
        EventEnvelope placed = placeOrder(PASTA_PLACE, MARGHERITA, TRUFFLE);

        assertRejected(placed, PASTA_PLACE, RejectReason.ITEM_UNAVAILABLE);
    }

    @Test
    void unknownItemRejectsWithItemUnavailable() {
        EventEnvelope placed = placeOrder(PASTA_PLACE, UUID.randomUUID());

        assertRejected(placed, PASTA_PLACE, RejectReason.ITEM_UNAVAILABLE);
    }

    @Test
    void atCapacityRejectsWithAtCapacityAndRestoringCapacityAcceptsAgain() {
        // one ticket is ACCEPTED, then maxConcurrentOrders is set to the current accepted count
        EventEnvelope first = placeOrder(PASTA_PLACE, MARGHERITA);
        awaitTicketStatus(first.orderId(), "ACCEPTED");
        long accepted = acceptedTickets(PASTA_PLACE);
        assertThat(accepted).isGreaterThanOrEqualTo(1);

        try {
            assertThat(patchRestaurant(PASTA_PLACE, "{\"maxConcurrentOrders\":" + accepted + "}").status())
                    .isEqualTo(200);

            EventEnvelope second = placeOrder(PASTA_PLACE, MARGHERITA);
            assertRejected(second, PASTA_PLACE, RejectReason.AT_CAPACITY);
        } finally {
            assertThat(patchRestaurant(PASTA_PLACE, "{\"maxConcurrentOrders\":5}").status()).isEqualTo(200);
        }

        EventEnvelope third = placeOrder(PASTA_PLACE, MARGHERITA);
        awaitEvents(Topics.RESTAURANT_EVENTS, third.orderId(), EventTypes.ORDER_ACCEPTED);
        awaitTicketStatus(third.orderId(), "ACCEPTED");
    }

    @Test
    void maxZeroRejectsEveryOrderWithAtCapacity() {
        try {
            assertThat(patchRestaurant(PASTA_PLACE, "{\"maxConcurrentOrders\":0}").status()).isEqualTo(200);
            EventEnvelope placed = placeOrder(PASTA_PLACE, MARGHERITA);

            assertRejected(placed, PASTA_PLACE, RejectReason.AT_CAPACITY);
        } finally {
            patchRestaurant(PASTA_PLACE, "{\"maxConcurrentOrders\":5}");
        }
    }

    @Test
    void unavailableItemWinsOverCapacity() {
        try {
            assertThat(patchRestaurant(PASTA_PLACE, "{\"maxConcurrentOrders\":0}").status()).isEqualTo(200);
            EventEnvelope placed = placeOrder(PASTA_PLACE, TRUFFLE);

            assertRejected(placed, PASTA_PLACE, RejectReason.ITEM_UNAVAILABLE);
        } finally {
            patchRestaurant(PASTA_PLACE, "{\"maxConcurrentOrders\":5}");
        }
    }
}
