package com.example.fooddelivery.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.events.CancelReason;
import com.example.fooddelivery.events.DeliveryFailureReason;
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

/**
 * Criteria 6 (both orderings of steps 4 and 5), 7 (quiet period), 8 (order side), 11/12 (stale) and the
 * premature convergence case. Events are produced as the restaurant and delivery services would.
 */
class OrderSagaIT extends AbstractOrderIT {

    private static final List<String> HAPPY_A = historyOf("PLACED", "ACCEPTED", "DRIVER_ASSIGNED",
            "READY_FOR_PICKUP", "OUT_FOR_DELIVERY", "DELIVERED");
    private static final List<String> HAPPY_B = historyOf("PLACED", "ACCEPTED", "READY_FOR_PICKUP",
            "DRIVER_ASSIGNED", "OUT_FOR_DELIVERY", "DELIVERED");

    @Test
    void happyPathOrderingA_driverAssignedBeforeReady() {
        UUID orderId = UUID.fromString(placeOrder().get("orderId").asText());
        EventEnvelope placed = awaitPlaced(orderId);
        UUID driver = UUID.randomUUID();

        EventEnvelope accepted = SagaEvents.accepted(placed);
        send(Topics.RESTAURANT_EVENTS, accepted);
        awaitStatus(orderId, "ACCEPTED");

        EventEnvelope assigned = SagaEvents.driverAssigned(accepted, driver);
        send(Topics.DELIVERY_EVENTS, assigned);
        awaitStatus(orderId, "DRIVER_ASSIGNED");
        assertThat(get("/orders/" + orderId).getBody().get("driverId").asText()).isEqualTo(driver.toString());

        EventEnvelope ready = SagaEvents.ready(placed);
        send(Topics.RESTAURANT_EVENTS, ready);
        awaitStatus(orderId, "READY_FOR_PICKUP");

        EventEnvelope pickedUp = SagaEvents.pickedUp(ready, driver);
        send(Topics.DELIVERY_EVENTS, pickedUp);
        awaitStatus(orderId, "OUT_FOR_DELIVERY");

        send(Topics.DELIVERY_EVENTS, SagaEvents.delivered(pickedUp, driver));
        awaitStatus(orderId, "DELIVERED");

        assertThat(history(orderId)).isEqualTo(HAPPY_A);
        assertThat(get("/orders/" + orderId).getBody().get("driverId").asText()).isEqualTo(driver.toString());
    }

    @Test
    void happyPathOrderingB_readyBeforeDriverAssigned() {
        UUID orderId = UUID.fromString(placeOrder().get("orderId").asText());
        EventEnvelope placed = awaitPlaced(orderId);
        UUID driver = UUID.randomUUID();

        EventEnvelope accepted = SagaEvents.accepted(placed);
        send(Topics.RESTAURANT_EVENTS, accepted);
        awaitStatus(orderId, "ACCEPTED");

        EventEnvelope ready = SagaEvents.ready(placed);
        send(Topics.RESTAURANT_EVENTS, ready);
        awaitStatus(orderId, "READY_FOR_PICKUP");

        EventEnvelope assigned = SagaEvents.driverAssigned(accepted, driver);
        send(Topics.DELIVERY_EVENTS, assigned);
        awaitStatus(orderId, "DRIVER_ASSIGNED");
        assertThat(get("/orders/" + orderId).getBody().get("driverId").asText()).isEqualTo(driver.toString());

        EventEnvelope pickedUp = SagaEvents.pickedUp(assigned, driver);
        send(Topics.DELIVERY_EVENTS, pickedUp);
        awaitStatus(orderId, "OUT_FOR_DELIVERY");

        send(Topics.DELIVERY_EVENTS, SagaEvents.delivered(pickedUp, driver));
        awaitStatus(orderId, "DELIVERED");

        assertThat(history(orderId)).isEqualTo(HAPPY_B);
    }

    @Test
    void rejectedOrderIsRejectedAndNeverCancelled() {
        UUID orderId = UUID.fromString(placeOrder().get("orderId").asText());
        EventEnvelope placed = awaitPlaced(orderId);

        send(Topics.RESTAURANT_EVENTS, SagaEvents.rejected(placed, RejectReason.CLOSED));
        awaitStatus(orderId, "REJECTED");

        assertThat(history(orderId)).isEqualTo(historyOf("PLACED", "REJECTED"));
        // quiet period: no OrderCancelled for this key
        Awaitility.await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(15))
                .until(() -> orderEventsOfType(orderId, "OrderCancelled").isEmpty());
        assertThat(status(orderId)).isEqualTo("REJECTED");
    }

    @Test
    void noDriverCancelsOrderAndEmitsOrderCancelledNoDriver() {
        UUID orderId = UUID.fromString(placeOrder().get("orderId").asText());
        EventEnvelope placed = awaitPlaced(orderId);
        EventEnvelope accepted = SagaEvents.accepted(placed);
        send(Topics.RESTAURANT_EVENTS, accepted);
        awaitStatus(orderId, "ACCEPTED");

        EventEnvelope failed = SagaEvents.deliveryFailed(accepted, DeliveryFailureReason.NO_DRIVER_AVAILABLE);
        send(Topics.DELIVERY_EVENTS, failed);
        awaitStatus(orderId, "CANCELLED");

        List<Received> cancelled = awaitOrderEvents(orderId, "OrderCancelled", 1);
        Awaitility.await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(10))
                .until(() -> orderEventsOfType(orderId, "OrderCancelled").size() == 1);
        Received rec = cancelled.get(0);
        assertThat(rec.key()).isEqualTo(orderId.toString());
        assertThat(rec.envelope().orderId()).isEqualTo(orderId);
        assertThat(rec.envelope().causationId()).isEqualTo(failed.eventId());
        assertThat(rec.envelope().correlationId()).isEqualTo(placed.correlationId());
        assertThat(((OrderCancelled) EventCodec.payloadOf(rec.envelope())).reason()).isEqualTo(CancelReason.NO_DRIVER);
        assertThat(rec.headers().get("eventType")).isEqualTo("OrderCancelled");
        assertThat(rec.headers().get("eventVersion")).isEqualTo("1");
        assertThat(rec.headers().get("correlationId")).isEqualTo(placed.correlationId().toString());
        assertThat(history(orderId)).isEqualTo(historyOf("PLACED", "ACCEPTED", "CANCELLED"));
    }

    @Test
    void staleOrderReadyAfterCancelledChangesNothing() {
        UUID orderId = UUID.fromString(placeOrder().get("orderId").asText());
        EventEnvelope placed = awaitPlaced(orderId);
        EventEnvelope accepted = SagaEvents.accepted(placed);
        send(Topics.RESTAURANT_EVENTS, accepted);
        awaitStatus(orderId, "ACCEPTED");
        send(Topics.DELIVERY_EVENTS, SagaEvents.deliveryFailed(accepted, DeliveryFailureReason.NO_DRIVER_AVAILABLE));
        awaitStatus(orderId, "CANCELLED");
        List<String> before = history(orderId);

        send(Topics.RESTAURANT_EVENTS, SagaEvents.ready(placed));

        Awaitility.await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(15)).until(
                () -> status(orderId).equals("CANCELLED") && history(orderId).equals(before));
        // no second OrderCancelled caused by the stale event
        assertThat(orderEventsOfType(orderId, "OrderCancelled")).hasSize(1);
    }

    @Test
    void prematureDriverAssignedBeforeAcceptedConvergesOnceAcceptedArrives() {
        UUID orderId = UUID.fromString(placeOrder().get("orderId").asText());
        EventEnvelope placed = awaitPlaced(orderId);
        UUID driver = UUID.randomUUID();
        EventEnvelope accepted = SagaEvents.accepted(placed);
        EventEnvelope assigned = SagaEvents.driverAssigned(accepted, driver);

        // the cross-topic race: DriverAssigned (delivery.events) overtakes OrderAccepted (restaurant.events)
        send(Topics.DELIVERY_EVENTS, assigned);
        send(Topics.RESTAURANT_EVENTS, accepted);

        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> assertThat(status(orderId)).isEqualTo("DRIVER_ASSIGNED"));
        assertThat(history(orderId)).isEqualTo(historyOf("PLACED", "ACCEPTED", "DRIVER_ASSIGNED"));
        assertThat(get("/orders/" + orderId).getBody().get("driverId").asText()).isEqualTo(driver.toString());
    }
}
