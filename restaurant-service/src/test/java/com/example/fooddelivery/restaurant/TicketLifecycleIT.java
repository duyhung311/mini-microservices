package com.example.fooddelivery.restaurant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.fooddelivery.events.DeliveryFailed;
import com.example.fooddelivery.events.DeliveryFailureReason;
import com.example.fooddelivery.events.Envelopes;
import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.OrderCancelled;
import com.example.fooddelivery.events.CancelReason;
import com.example.fooddelivery.events.OrderReady;
import com.example.fooddelivery.events.Topics;
import com.example.fooddelivery.restaurant.support.RestaurantIntegrationTestBase;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Plan constraints "Restaurant tickets" and "REST > Restaurant": POST /tickets/{id}/ready and its 409s. */
class TicketLifecycleIT extends RestaurantIntegrationTestBase {

    @Test
    void readyOnAcceptedTicketReturns200BecomesReadyAndPublishesOrderReady() {
        EventEnvelope placed = placeOrder(PASTA_PLACE, MARGHERITA);
        awaitTicketStatus(placed.orderId(), "ACCEPTED");

        Resp resp = markReady(placed.orderId());

        assertThat(resp.status()).isEqualTo(200);
        assertThat(resp.body().get("orderId").asText()).isEqualTo(placed.orderId().toString());
        assertThat(resp.body().get("restaurantId").asText()).isEqualTo(PASTA_PLACE.toString());
        assertThat(resp.body().get("status").asText()).isEqualTo("READY");
        assertThat(ticketStatus(placed.orderId())).isEqualTo("READY");

        List<Published> ready = awaitEvents(Topics.RESTAURANT_EVENTS, placed.orderId(), EventTypes.ORDER_READY);
        Published record = ready.get(0);
        assertThat(record.key()).isEqualTo(placed.orderId().toString());
        EventEnvelope out = record.envelope();
        assertThat(out.orderId()).isEqualTo(placed.orderId());
        assertThat(out.eventVersion()).isEqualTo(1);
        // REST-originated: causationId is null, correlationId is that of the original OrderPlaced
        assertThat(out.causationId()).isNull();
        assertThat(out.correlationId()).isEqualTo(placed.correlationId());
        assertThat(record.headers().get("eventType")).isEqualTo("OrderReady");
        assertThat(record.headers().get("eventVersion")).isEqualTo("1");
        assertThat(record.headers().get("correlationId")).isEqualTo(placed.correlationId().toString());
        OrderReady payload = (OrderReady) EventCodec.payloadOf(out);
        assertThat(payload.restaurantId()).isEqualTo(PASTA_PLACE);
        assertThat(payload.readyAt()).isNotNull();

        // the serialised envelope always carries the causationId field, as null
        assertThat(EventCodec.toJson(out)).contains("\"causationId\":null");
    }

    @Test
    void secondReadyReturns409AndPublishesNoSecondOrderReady() {
        EventEnvelope placed = placeOrder(PASTA_PLACE, MARGHERITA);
        awaitTicketStatus(placed.orderId(), "ACCEPTED");
        assertThat(markReady(placed.orderId()).status()).isEqualTo(200);
        awaitEvents(Topics.RESTAURANT_EVENTS, placed.orderId(), EventTypes.ORDER_READY);

        Resp second = markReady(placed.orderId());

        assertThat(second.status()).isEqualTo(409);
        assertThat(second.contentType()).startsWith("application/problem+json");
        assertThat(ticketStatus(placed.orderId())).isEqualTo("READY");
        await().during(QUIET).atMost(WAIT).pollInterval(POLL).until(
                () -> eventsOfType(Topics.RESTAURANT_EVENTS, placed.orderId(), EventTypes.ORDER_READY).size() == 1);
    }

    @Test
    void readyOnAbandonedTicketReturns409() {
        EventEnvelope placed = placeOrder(PASTA_PLACE, MARGHERITA);
        awaitTicketStatus(placed.orderId(), "ACCEPTED");
        publish(Topics.DELIVERY_EVENTS, Envelopes.next(placed, EventTypes.DELIVERY_FAILED,
                new DeliveryFailed(DeliveryFailureReason.NO_DRIVER_AVAILABLE)));
        awaitTicketStatus(placed.orderId(), "ABANDONED");

        Resp resp = markReady(placed.orderId());

        assertThat(resp.status()).isEqualTo(409);
        assertThat(resp.contentType()).startsWith("application/problem+json");
        assertThat(ticketStatus(placed.orderId())).isEqualTo("ABANDONED");
        assertThat(eventsOfType(Topics.RESTAURANT_EVENTS, placed.orderId(), EventTypes.ORDER_READY)).isEmpty();
    }

    @Test
    void readyOnCancelledTicketReturns409() {
        EventEnvelope placed = placeOrder(PASTA_PLACE, MARGHERITA);
        awaitTicketStatus(placed.orderId(), "ACCEPTED");
        publish(Topics.ORDER_EVENTS, Envelopes.next(placed, EventTypes.ORDER_CANCELLED,
                new OrderCancelled(CancelReason.CUSTOMER_REQUEST)));
        awaitTicketStatus(placed.orderId(), "CANCELLED");

        Resp resp = markReady(placed.orderId());

        assertThat(resp.status()).isEqualTo(409);
        assertThat(ticketStatus(placed.orderId())).isEqualTo("CANCELLED");
        assertThat(eventsOfType(Topics.RESTAURANT_EVENTS, placed.orderId(), EventTypes.ORDER_READY)).isEmpty();
    }

    @Test
    void rejectedOrderHasNoTicketSoReadyReturns404() {
        EventEnvelope placed = placeOrder(NIGHT_GRILL, MARGHERITA);
        awaitEvents(Topics.RESTAURANT_EVENTS, placed.orderId(), EventTypes.ORDER_REJECTED);

        assertThat(markReady(placed.orderId()).status()).isEqualTo(404);
    }
}
