package com.example.fooddelivery.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.events.CancelReason;
import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.OrderCancelled;
import com.example.fooddelivery.events.RejectReason;
import com.example.fooddelivery.events.Topics;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.ResponseEntity;

/** Criterion 9, order side: POST /orders/{id}/cancel is 200 / 409 / 404 depending on state. */
class OrderCancelIT extends AbstractOrderIT {

    private record Ctx(UUID orderId, EventEnvelope placed) {
    }

    private Ctx newOrder() {
        UUID orderId = UUID.fromString(placeOrder().get("orderId").asText());
        return new Ctx(orderId, awaitPlaced(orderId));
    }

    /** Drives a fresh order to the given status by producing the needed events. */
    private Ctx orderIn(String target) {
        Ctx c = newOrder();
        if (target.equals("PLACED")) {
            return c;
        }
        if (target.equals("REJECTED")) {
            send(Topics.RESTAURANT_EVENTS, SagaEvents.rejected(c.placed(), RejectReason.AT_CAPACITY));
            awaitStatus(c.orderId(), "REJECTED");
            return c;
        }
        EventEnvelope accepted = SagaEvents.accepted(c.placed());
        send(Topics.RESTAURANT_EVENTS, accepted);
        awaitStatus(c.orderId(), "ACCEPTED");
        if (target.equals("ACCEPTED")) {
            return c;
        }
        UUID driver = UUID.randomUUID();
        EventEnvelope assigned = SagaEvents.driverAssigned(accepted, driver);
        EventEnvelope ready = SagaEvents.ready(c.placed());
        if (target.equals("READY_FOR_PICKUP")) {
            send(Topics.RESTAURANT_EVENTS, ready);
            awaitStatus(c.orderId(), "READY_FOR_PICKUP");
            return c;
        }
        send(Topics.DELIVERY_EVENTS, assigned);
        awaitStatus(c.orderId(), "DRIVER_ASSIGNED");
        if (target.equals("DRIVER_ASSIGNED")) {
            return c;
        }
        send(Topics.RESTAURANT_EVENTS, ready);
        awaitStatus(c.orderId(), "READY_FOR_PICKUP");
        EventEnvelope pickedUp = SagaEvents.pickedUp(ready, driver);
        send(Topics.DELIVERY_EVENTS, pickedUp);
        awaitStatus(c.orderId(), "OUT_FOR_DELIVERY");
        if (target.equals("OUT_FOR_DELIVERY")) {
            return c;
        }
        send(Topics.DELIVERY_EVENTS, SagaEvents.delivered(pickedUp, driver));
        awaitStatus(c.orderId(), "DELIVERED");
        return c;
    }

    @ParameterizedTest(name = "cancel in {0} returns 200 and publishes OrderCancelled(CUSTOMER_REQUEST)")
    @ValueSource(strings = {"PLACED", "ACCEPTED", "DRIVER_ASSIGNED", "READY_FOR_PICKUP"})
    void cancellableStatesReturn200AndPublishCustomerRequest(String state) {
        Ctx c = orderIn(state);

        ResponseEntity<JsonNode> r = post("/orders/" + c.orderId() + "/cancel");

        assertThat(r.getStatusCode().value()).isEqualTo(200);
        assertThat(r.getBody().get("orderId").asText()).isEqualTo(c.orderId().toString());
        assertThat(r.getBody().get("status").asText()).isEqualTo("CANCELLED");
        assertThat(status(c.orderId())).isEqualTo("CANCELLED");

        List<Received> cancelled = awaitOrderEvents(c.orderId(), "OrderCancelled", 1);
        Awaitility.await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(10))
                .until(() -> orderEventsOfType(c.orderId(), "OrderCancelled").size() == 1);
        Received rec = cancelled.get(0);
        assertThat(rec.key()).isEqualTo(c.orderId().toString());
        assertThat(((OrderCancelled) EventCodec.payloadOf(rec.envelope())).reason())
                .isEqualTo(CancelReason.CUSTOMER_REQUEST);
        assertThat(rec.envelope().causationId()).as("REST-originated").isNull();
        assertThat(rec.envelope().correlationId()).isEqualTo(c.placed().correlationId());
        assertThat(rec.headers().get("eventType")).isEqualTo("OrderCancelled");
        assertThat(rec.headers().get("correlationId")).isEqualTo(c.placed().correlationId().toString());
        assertThat(history(c.orderId()).get(history(c.orderId()).size() - 1)).endsWith(">CANCELLED");
    }

    @ParameterizedTest(name = "cancel in {0} returns 409 and publishes nothing")
    @ValueSource(strings = {"OUT_FOR_DELIVERY", "DELIVERED", "REJECTED"})
    void nonCancellableStatesReturn409(String state) {
        Ctx c = orderIn(state);
        List<String> before = history(c.orderId());

        ResponseEntity<JsonNode> r = post("/orders/" + c.orderId() + "/cancel");

        assertThat(r.getStatusCode().value()).isEqualTo(409);
        assertThat(r.getBody().path("status").asInt()).isEqualTo(409);
        assertThat(status(c.orderId())).isEqualTo(state);
        assertThat(history(c.orderId())).isEqualTo(before);
        Awaitility.await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(15))
                .until(() -> orderEventsOfType(c.orderId(), "OrderCancelled").isEmpty());
    }

    @Test
    void cancelInCancelledReturns409AndDoesNotPublishASecondEvent() {
        Ctx c = orderIn("ACCEPTED");
        assertThat(post("/orders/" + c.orderId() + "/cancel").getStatusCode().value()).isEqualTo(200);
        awaitOrderEvents(c.orderId(), "OrderCancelled", 1);

        ResponseEntity<JsonNode> second = post("/orders/" + c.orderId() + "/cancel");

        assertThat(second.getStatusCode().value()).isEqualTo(409);
        Awaitility.await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(15))
                .until(() -> orderEventsOfType(c.orderId(), "OrderCancelled").size() == 1);
        assertThat(status(c.orderId())).isEqualTo("CANCELLED");
    }

    @Test
    void cancelUnknownOrderReturns404() {
        ResponseEntity<JsonNode> r = post("/orders/" + UUID.randomUUID() + "/cancel");
        assertThat(r.getStatusCode().value()).isEqualTo(404);
    }
}
