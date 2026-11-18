package com.example.fooddelivery.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.fooddelivery.delivery.support.DeliveryIntegrationTestBase;
import com.example.fooddelivery.events.DeliveryFailed;
import com.example.fooddelivery.events.DeliveryFailureReason;
import com.example.fooddelivery.events.DriverAssigned;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.Topics;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Criteria 3 and 4: OrderAccepted assigns the lowest AVAILABLE driver, or fails with NO_DRIVER_AVAILABLE. */
class DriverAssignmentIT extends DeliveryIntegrationTestBase {

    @Test
    void placedThenAcceptedAssignsLowestAvailableDriverExactlyOnce() {
        UUID orderId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        EventEnvelope placed = sendPlaced(orderId, correlationId);
        EventEnvelope accepted = sendAccepted(placed);

        List<Published> assigned = awaitDeliveryEvents(orderId, EventTypes.DRIVER_ASSIGNED);
        Published record = assigned.get(0);
        assertEnvelope(record, EventTypes.DRIVER_ASSIGNED, orderId, correlationId, accepted.eventId());
        DriverAssigned payload = (DriverAssigned) record.payload();
        assertThat(payload.driverId()).isEqualTo(DRIVER_1);
        assertThat(payload.driverName()).isEqualTo(driverName(DRIVER_1));
        assertThat(payload.etaMinutes()).isEqualTo(20);

        assertThat(driverStatus(DRIVER_1)).isEqualTo("BUSY");
        assertThat(driverStatus(DRIVER_2)).isEqualTo("AVAILABLE");

        JsonNode delivery = get("/deliveries/" + orderId).body();
        assertThat(delivery.get("status").asText()).isEqualTo("ASSIGNED");
        assertThat(delivery.get("driverId").asText()).isEqualTo(DRIVER_1.toString());
        assertThat(delivery.get("pickupAddress").asText()).isEqualTo(PICKUP_ADDRESS);
        assertThat(delivery.get("dropoffAddress").asText()).isEqualTo(DROPOFF_ADDRESS);

        // exactly one DriverAssigned, and every record of the order sits on one partition with key = orderId
        await().during(QUIET).atMost(WAIT).pollInterval(POLL)
                .until(() -> deliveryEvents(orderId, EventTypes.DRIVER_ASSIGNED).size() == 1);
        List<Published> all = eventsFor(Topics.DELIVERY_EVENTS, orderId);
        assertThat(all).extracting(Published::partition).containsOnly(record.partition());
        assertThat(all).extracting(Published::key).containsOnly(orderId.toString());
    }

    @Test
    void picksLowestIdAmongAvailableDriversOnly() {
        setDriver(DRIVER_1, "OFFLINE");
        UUID orderId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        EventEnvelope accepted = sendAccepted(sendPlaced(orderId, correlationId));

        Published record = awaitDeliveryEvents(orderId, EventTypes.DRIVER_ASSIGNED).get(0);
        assertEnvelope(record, EventTypes.DRIVER_ASSIGNED, orderId, correlationId, accepted.eventId());
        assertThat(((DriverAssigned) record.payload()).driverId()).isEqualTo(DRIVER_2);
        assertThat(driverStatus(DRIVER_2)).isEqualTo("BUSY");
        assertThat(driverStatus(DRIVER_1)).isEqualTo("OFFLINE");
    }

    @Test
    void secondOrderGetsNextDriverWhileFirstIsBusy() {
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();
        sendAccepted(sendPlaced(first, UUID.randomUUID()));
        awaitDeliveryStatus(first, "ASSIGNED");
        sendAccepted(sendPlaced(second, UUID.randomUUID()));
        awaitDeliveryStatus(second, "ASSIGNED");

        assertThat(deliveryDriverId(first)).isEqualTo(DRIVER_1);
        assertThat(deliveryDriverId(second)).isEqualTo(DRIVER_2);
    }

    @Test
    void acceptedWithoutPriorPlacedStillAssignsDriver() {
        UUID orderId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        EventEnvelope accepted = sendAcceptedAlone(orderId, correlationId);

        Published record = awaitDeliveryEvents(orderId, EventTypes.DRIVER_ASSIGNED).get(0);
        assertEnvelope(record, EventTypes.DRIVER_ASSIGNED, orderId, correlationId, accepted.eventId());
        assertThat(((DriverAssigned) record.payload()).driverId()).isEqualTo(DRIVER_1);
        awaitDeliveryStatus(orderId, "ASSIGNED");
        assertThat(driverStatus(DRIVER_1)).isEqualTo("BUSY");
        assertThat(get("/deliveries/" + orderId).body().get("pickupAddress").asText()).isEqualTo(PICKUP_ADDRESS);
    }

    @Test
    void noAvailableDriverFailsDeliveryWithNoDriverAvailable() {
        setAllDrivers("OFFLINE");
        UUID orderId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        EventEnvelope accepted = sendAccepted(sendPlaced(orderId, correlationId));

        Published record = awaitDeliveryEvents(orderId, EventTypes.DELIVERY_FAILED).get(0);
        assertEnvelope(record, EventTypes.DELIVERY_FAILED, orderId, correlationId, accepted.eventId());
        assertThat(((DeliveryFailed) record.payload()).reason()).isEqualTo(DeliveryFailureReason.NO_DRIVER_AVAILABLE);
        awaitDeliveryStatus(orderId, "FAILED");
        assertThat(countDriversWithStatus("OFFLINE")).isEqualTo(5);
        assertNoDeliveryEvent(orderId, EventTypes.DRIVER_ASSIGNED);
        assertThat(deliveryEvents(orderId, EventTypes.DELIVERY_FAILED)).hasSize(1);
    }

    @Test
    void allDriversBusyAlsoFailsWithNoDriverAvailable() {
        setAllDrivers("BUSY");
        UUID orderId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        EventEnvelope accepted = sendAccepted(sendPlaced(orderId, correlationId));

        Published record = awaitDeliveryEvents(orderId, EventTypes.DELIVERY_FAILED).get(0);
        assertEnvelope(record, EventTypes.DELIVERY_FAILED, orderId, correlationId, accepted.eventId());
        assertThat(((DeliveryFailed) record.payload()).reason()).isEqualTo(DeliveryFailureReason.NO_DRIVER_AVAILABLE);
        awaitDeliveryStatus(orderId, "FAILED");
    }

    @Test
    void orderRejectedAloneIsIgnored() {
        UUID orderId = UUID.randomUUID();
        sendRejected(unpublishedPlaced(orderId, UUID.randomUUID()));

        await().during(QUIET).atMost(WAIT).pollInterval(POLL)
                .until(() -> eventsFor(Topics.DELIVERY_EVENTS, orderId).isEmpty()
                        && deliveryStatus(orderId) == null);
        assertThat(countDriversWithStatus("AVAILABLE")).isEqualTo(5);
    }

    @Test
    void orderRejectedAfterPlacedPublishesNothingAndAssignsNoDriver() {
        UUID orderId = UUID.randomUUID();
        EventEnvelope placed = sendPlaced(orderId, UUID.randomUUID());
        awaitDeliveryStatus(orderId, "PENDING");
        sendRejected(placed);

        await().during(QUIET).atMost(WAIT).pollInterval(POLL)
                .until(() -> eventsFor(Topics.DELIVERY_EVENTS, orderId).isEmpty()
                        && "PENDING".equals(deliveryStatus(orderId)));
        assertThat(countDriversWithStatus("AVAILABLE")).isEqualTo(5);
    }
}
