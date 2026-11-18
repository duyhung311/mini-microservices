package com.example.fooddelivery.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.fooddelivery.delivery.support.DeliveryIntegrationTestBase;
import com.example.fooddelivery.events.DeliveryFailed;
import com.example.fooddelivery.events.DeliveryFailureReason;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.Topics;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Criteria 8 and 9, delivery side: cancel handling and the DRIVER_CANCELLED failure. */
class DeliveryCompensationIT extends DeliveryIntegrationTestBase {

    @Test
    void cancelWhileAssignedCancelsDeliveryAndFreesDriver() {
        UUID orderId = UUID.randomUUID();
        EventEnvelope[] saga = assignedOrder(orderId, UUID.randomUUID());
        assertThat(driverStatus(DRIVER_1)).isEqualTo("BUSY");

        sendCancelled(saga[0]);

        awaitDeliveryStatus(orderId, "CANCELLED");
        awaitDriverStatus(DRIVER_1, "AVAILABLE");
        assertThat(countDriversWithStatus("AVAILABLE")).isEqualTo(5);
    }

    @Test
    void cancelWhileReadyForPickupCancelsDeliveryAndFreesDriver() {
        UUID orderId = UUID.randomUUID();
        EventEnvelope[] saga = assignedOrder(orderId, UUID.randomUUID());
        sendReady(saga[1]);
        awaitDeliveryStatus(orderId, "READY_FOR_PICKUP");

        sendCancelled(saga[0]);

        awaitDeliveryStatus(orderId, "CANCELLED");
        awaitDriverStatus(DRIVER_1, "AVAILABLE");
        assertThat(countDriversWithStatus("AVAILABLE")).isEqualTo(5);
    }

    @Test
    void cancelAfterPlacedButBeforeAcceptedLeavesTombstoneAndLaterAcceptedAssignsNoDriver() {
        UUID orderId = UUID.randomUUID();
        EventEnvelope placed = sendPlaced(orderId, UUID.randomUUID());
        awaitDeliveryStatus(orderId, "PENDING");

        sendCancelled(placed);
        awaitDeliveryStatus(orderId, "CANCELLED");

        sendAccepted(placed);
        assertNoDriverAssignedAndAllAvailable(orderId);
    }

    @Test
    void cancelWithNoRowYetLeavesTombstoneReadableOverRestAndLaterAcceptedAssignsNoDriver() {
        UUID orderId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        EventEnvelope cancelled = sendCancelledAlone(orderId, correlationId);
        awaitDeliveryStatus(orderId, "CANCELLED");

        Resp tombstone = get("/deliveries/" + orderId);
        assertThat(tombstone.status()).isEqualTo(200);
        assertThat(tombstone.body().get("status").asText()).isEqualTo("CANCELLED");
        assertThat(tombstone.body().get("driverId") == null || tombstone.body().get("driverId").isNull()).isTrue();

        sendAccepted(cancelled);
        assertNoDriverAssignedAndAllAvailable(orderId);
    }

    @Test
    void cancelOnFailedDeliveryIsNoOp() {
        setAllDrivers("OFFLINE");
        UUID orderId = UUID.randomUUID();
        EventEnvelope placed = sendPlaced(orderId, UUID.randomUUID());
        sendAccepted(placed);
        awaitDeliveryStatus(orderId, "FAILED");
        awaitDeliveryEvents(orderId, EventTypes.DELIVERY_FAILED);
        setAllDrivers("AVAILABLE");

        sendCancelled(placed);

        assertDeliveryStaysAt(orderId, "FAILED");
        assertThat(deliveryEvents(orderId, EventTypes.DELIVERY_FAILED)).hasSize(1);
        assertThat(countDriversWithStatus("AVAILABLE")).isEqualTo(5);
    }

    @Test
    void cancelOnPickedUpDeliveryIsIgnored() {
        UUID orderId = UUID.randomUUID();
        EventEnvelope[] saga = assignedOrder(orderId, UUID.randomUUID());
        sendReady(saga[1]);
        awaitDeliveryStatus(orderId, "READY_FOR_PICKUP");
        assertThat(pickup(orderId).status()).isEqualTo(200);

        sendCancelled(saga[0]);

        assertDeliveryStaysAt(orderId, "PICKED_UP");
        assertThat(driverStatus(DRIVER_1)).isEqualTo("BUSY");
    }

    @Test
    void driverGoingOfflineWhileAssignedFailsDeliveryWithDriverCancelled() {
        UUID orderId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        assignedOrder(orderId, correlationId);

        Resp r = patchDriverStatus(DRIVER_1, "OFFLINE");

        assertThat(r.status()).isEqualTo(200);
        assertThat(r.body().get("status").asText()).isEqualTo("OFFLINE");
        assertDriverCancelled(orderId, correlationId);
    }

    @Test
    void driverGoingOfflineWhileReadyForPickupFailsDeliveryWithDriverCancelled() {
        UUID orderId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        EventEnvelope[] saga = assignedOrder(orderId, correlationId);
        sendReady(saga[1]);
        awaitDeliveryStatus(orderId, "READY_FOR_PICKUP");

        Resp r = patchDriverStatus(DRIVER_1, "OFFLINE");

        assertThat(r.status()).isEqualTo(200);
        assertDriverCancelled(orderId, correlationId);
    }

    private void assertDriverCancelled(UUID orderId, UUID correlationId) {
        Published failed = awaitDeliveryEvents(orderId, EventTypes.DELIVERY_FAILED).get(0);
        assertEnvelope(failed, EventTypes.DELIVERY_FAILED, orderId, correlationId, null);
        assertThat(((DeliveryFailed) failed.payload()).reason()).isEqualTo(DeliveryFailureReason.DRIVER_CANCELLED);
        awaitDeliveryStatus(orderId, "FAILED");
        assertThat(driverStatus(DRIVER_1)).isEqualTo("OFFLINE");
        assertThat(deliveryEvents(orderId, EventTypes.DELIVERY_FAILED)).hasSize(1);
    }

    private void assertNoDriverAssignedAndAllAvailable(UUID orderId) {
        await().during(QUIET).atMost(WAIT).pollInterval(POLL)
                .until(() -> deliveryEvents(orderId, EventTypes.DRIVER_ASSIGNED).isEmpty()
                        && eventsFor(Topics.DELIVERY_EVENTS, orderId).isEmpty()
                        && "CANCELLED".equals(deliveryStatus(orderId))
                        && countDriversWithStatus("AVAILABLE") == 5);
    }
}
