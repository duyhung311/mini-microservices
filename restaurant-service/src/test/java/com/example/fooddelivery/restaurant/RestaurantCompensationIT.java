package com.example.fooddelivery.restaurant;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.events.CancelReason;
import com.example.fooddelivery.events.DeliveryFailed;
import com.example.fooddelivery.events.DeliveryFailureReason;
import com.example.fooddelivery.events.Envelopes;
import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.OrderCancelled;
import com.example.fooddelivery.events.OrderPlaced;
import com.example.fooddelivery.events.OrderRejected;
import com.example.fooddelivery.events.RejectReason;
import com.example.fooddelivery.events.Topics;
import com.example.fooddelivery.restaurant.support.RestaurantIntegrationTestBase;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Criterion 8, restaurant side: both arrival orders of DeliveryFailed and OrderCancelled end CANCELLED. */
class RestaurantCompensationIT extends RestaurantIntegrationTestBase {

    private EventEnvelope acceptedOrder() {
        EventEnvelope placed = placeOrder(PASTA_PLACE, MARGHERITA);
        awaitTicketStatus(placed.orderId(), "ACCEPTED");
        return placed;
    }

    private void deliveryFailed(EventEnvelope placed, DeliveryFailureReason reason) {
        publish(Topics.DELIVERY_EVENTS,
                Envelopes.next(placed, EventTypes.DELIVERY_FAILED, new DeliveryFailed(reason)));
    }

    private void orderCancelled(EventEnvelope placed, CancelReason reason) {
        publish(Topics.ORDER_EVENTS,
                Envelopes.next(placed, EventTypes.ORDER_CANCELLED, new OrderCancelled(reason)));
    }

    @Test
    void deliveryFailedThenOrderCancelledGoesAbandonedThenCancelled() {
        EventEnvelope placed = acceptedOrder();

        deliveryFailed(placed, DeliveryFailureReason.NO_DRIVER_AVAILABLE);
        awaitTicketStatus(placed.orderId(), "ABANDONED");
        assertTicketStaysAt(placed.orderId(), "ABANDONED");

        orderCancelled(placed, CancelReason.NO_DRIVER);
        awaitTicketStatus(placed.orderId(), "CANCELLED");
        assertTicketStaysAt(placed.orderId(), "CANCELLED");
    }

    @Test
    void orderCancelledThenDeliveryFailedStaysCancelled() {
        EventEnvelope placed = acceptedOrder();

        orderCancelled(placed, CancelReason.NO_DRIVER);
        awaitTicketStatus(placed.orderId(), "CANCELLED");

        deliveryFailed(placed, DeliveryFailureReason.NO_DRIVER_AVAILABLE);
        assertTicketStaysAt(placed.orderId(), "CANCELLED");
    }

    @Test
    void driverCancelledFailureAlsoAbandonsTheTicket() {
        EventEnvelope placed = acceptedOrder();

        deliveryFailed(placed, DeliveryFailureReason.DRIVER_CANCELLED);
        awaitTicketStatus(placed.orderId(), "ABANDONED");
        orderCancelled(placed, CancelReason.NO_DRIVER);
        awaitTicketStatus(placed.orderId(), "CANCELLED");
    }

    @Test
    void customerCancelOnReadyTicketGivesCancelled() {
        EventEnvelope placed = acceptedOrder();
        assertThat(markReady(placed.orderId()).status()).isEqualTo(200);
        awaitTicketStatus(placed.orderId(), "READY");

        orderCancelled(placed, CancelReason.CUSTOMER_REQUEST);

        awaitTicketStatus(placed.orderId(), "CANCELLED");
        assertTicketStaysAt(placed.orderId(), "CANCELLED");
    }

    @Test
    void customerCancelOnAcceptedTicketGivesCancelled() {
        EventEnvelope placed = acceptedOrder();

        orderCancelled(placed, CancelReason.CUSTOMER_REQUEST);

        awaitTicketStatus(placed.orderId(), "CANCELLED");
    }

    @Test
    void deliveryFailedOnReadyTicketGoesAbandonedThenCancelled() {
        EventEnvelope placed = acceptedOrder();
        assertThat(markReady(placed.orderId()).status()).isEqualTo(200);

        deliveryFailed(placed, DeliveryFailureReason.DRIVER_CANCELLED);
        awaitTicketStatus(placed.orderId(), "ABANDONED");

        orderCancelled(placed, CancelReason.NO_DRIVER);
        awaitTicketStatus(placed.orderId(), "CANCELLED");
    }

    @Test
    void abandonedTicketFreesCapacity() {
        EventEnvelope first = acceptedOrder();
        long accepted = acceptedTickets(PASTA_PLACE);
        assertThat(accepted).isEqualTo(1L);

        try {
            // capacity is now exactly used up
            assertThat(patchRestaurant(PASTA_PLACE, "{\"maxConcurrentOrders\":" + accepted + "}").status())
                    .isEqualTo(200);
            EventEnvelope blocked = placeOrder(PASTA_PLACE, MARGHERITA);
            Published rejected = awaitEvents(Topics.RESTAURANT_EVENTS, blocked.orderId(),
                    EventTypes.ORDER_REJECTED).get(0);
            assertThat(((OrderRejected) EventCodec.payloadOf(rejected.envelope())).reason())
                    .isEqualTo(RejectReason.AT_CAPACITY);

            // abandoning the first ticket frees the slot
            deliveryFailed(first, DeliveryFailureReason.NO_DRIVER_AVAILABLE);
            awaitTicketStatus(first.orderId(), "ABANDONED");

            EventEnvelope admitted = placeOrder(PASTA_PLACE, MARGHERITA);
            awaitEvents(Topics.RESTAURANT_EVENTS, admitted.orderId(), EventTypes.ORDER_ACCEPTED);
            awaitTicketStatus(admitted.orderId(), "ACCEPTED");
        } finally {
            patchRestaurant(PASTA_PLACE, "{\"maxConcurrentOrders\":5}");
        }
    }

    @Test
    void unknownTicketOnCancelOrDeliveryFailedIsSkippedAndTheListenerKeepsWorking() {
        UUID ghostOrder = UUID.randomUUID();
        EventEnvelope ghostPlaced = Envelopes.first(EventTypes.ORDER_PLACED, ghostOrder, UUID.randomUUID(),
                new OrderPlaced(UUID.randomUUID(), PASTA_PLACE, List.of(),
                        BigDecimal.ZERO, "nowhere"));
        // events for an order the restaurant never saw (no OrderPlaced was published for it)
        orderCancelled(ghostPlaced, CancelReason.TIMEOUT);
        deliveryFailed(ghostPlaced, DeliveryFailureReason.NO_DRIVER_AVAILABLE);

        // a real order afterwards is still processed
        EventEnvelope placed = placeOrder(PASTA_PLACE, MARGHERITA);
        awaitTicketStatus(placed.orderId(), "ACCEPTED");
        assertThat(ticketStatus(ghostOrder)).isNull();
    }
}
