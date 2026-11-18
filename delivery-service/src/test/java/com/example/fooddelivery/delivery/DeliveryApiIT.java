package com.example.fooddelivery.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.delivery.support.DeliveryIntegrationTestBase;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.OrderDelivered;
import com.example.fooddelivery.events.OrderPickedUp;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Criterion 19, delivery side: every status code of REST > Delivery, plus the pickup guard. */
class DeliveryApiIT extends DeliveryIntegrationTestBase {

    // ------------------------------------------------------------------ GET /drivers, PATCH /drivers/{id}

    @Test
    void listDriversReturnsFiveSeededDrivers() {
        Resp r = get("/drivers");
        assertThat(r.status()).isEqualTo(200);
        assertThat(r.body().isArray()).isTrue();
        assertThat(r.body()).hasSize(5);
        for (JsonNode d : r.body()) {
            assertThat(d.has("id")).isTrue();
            assertThat(d.get("name").asText()).isNotBlank();
            assertThat(d.get("status").asText()).isEqualTo("AVAILABLE");
        }
        assertThat(driverStatus(DRIVER_1)).isEqualTo("AVAILABLE");
    }

    @Test
    void patchAvailableToOfflineAndBackReturns200WithDriver() {
        Resp off = patchDriverStatus(DRIVER_1, "OFFLINE");
        assertThat(off.status()).isEqualTo(200);
        assertThat(off.body().get("id").asText()).isEqualTo(DRIVER_1.toString());
        assertThat(off.body().get("status").asText()).isEqualTo("OFFLINE");
        assertThat(driverStatus(DRIVER_1)).isEqualTo("OFFLINE");

        Resp on = patchDriverStatus(DRIVER_1, "AVAILABLE");
        assertThat(on.status()).isEqualTo(200);
        assertThat(on.body().get("status").asText()).isEqualTo("AVAILABLE");
        assertThat(driverStatus(DRIVER_1)).isEqualTo("AVAILABLE");
    }

    @Test
    void patchWithBusyInBodyReturns400() {
        Resp r = patchDriverStatus(DRIVER_1, "BUSY");
        assertThat(r.status()).isEqualTo(400);
        assertThat(driverStatus(DRIVER_1)).isEqualTo("AVAILABLE");
    }

    @Test
    void patchWithMissingStatusReturns400() {
        assertThat(patchDriver(DRIVER_1, "{}").status()).isEqualTo(400);
    }

    @Test
    void patchWithInvalidStatusReturns400() {
        assertThat(patchDriverStatus(DRIVER_1, "FLYING").status()).isEqualTo(400);
        assertThat(driverStatus(DRIVER_1)).isEqualTo("AVAILABLE");
    }

    @Test
    void patchUnknownDriverReturns404() {
        Resp r = patchDriverStatus(UUID.randomUUID(), "OFFLINE");
        assertThat(r.status()).isEqualTo(404);
        assertThat(r.contentType()).contains("problem+json");
    }

    @Test
    void patchBusyDriverToAvailableReturns409() {
        UUID orderId = UUID.randomUUID();
        assignedOrder(orderId, UUID.randomUUID());
        assertThat(driverStatus(DRIVER_1)).isEqualTo("BUSY");

        Resp r = patchDriverStatus(DRIVER_1, "AVAILABLE");
        assertThat(r.status()).isEqualTo(409);
        assertThat(r.contentType()).contains("problem+json");
        assertThat(driverStatus(DRIVER_1)).isEqualTo("BUSY");
        assertThat(deliveryStatus(orderId)).isEqualTo("ASSIGNED");
    }

    @Test
    void patchBusyDriverToOfflineWithPickedUpDeliveryReturns409() {
        UUID orderId = UUID.randomUUID();
        EventEnvelope[] saga = assignedOrder(orderId, UUID.randomUUID());
        sendReady(saga[1]);
        awaitDeliveryStatus(orderId, "READY_FOR_PICKUP");
        assertThat(pickup(orderId).status()).isEqualTo(200);

        Resp r = patchDriverStatus(DRIVER_1, "OFFLINE");
        assertThat(r.status()).isEqualTo(409);
        assertThat(driverStatus(DRIVER_1)).isEqualTo("BUSY");
        assertThat(deliveryStatus(orderId)).isEqualTo("PICKED_UP");
        assertNoDeliveryEvent(orderId, EventTypes.DELIVERY_FAILED);
    }

    // ------------------------------------------------------------------ GET /deliveries/{orderId}

    @Test
    void getUnknownDeliveryReturns404() {
        Resp r = get("/deliveries/" + UUID.randomUUID());
        assertThat(r.status()).isEqualTo(404);
        assertThat(r.contentType()).contains("problem+json");
    }

    @Test
    void getDeliveryReturnsDocumentedFields() {
        UUID orderId = UUID.randomUUID();
        assignedOrder(orderId, UUID.randomUUID());

        Resp r = get("/deliveries/" + orderId);
        assertThat(r.status()).isEqualTo(200);
        JsonNode body = r.body();
        assertThat(body.get("orderId").asText()).isEqualTo(orderId.toString());
        assertThat(body.get("status").asText()).isEqualTo("ASSIGNED");
        assertThat(body.get("driverId").asText()).isEqualTo(DRIVER_1.toString());
        assertThat(body.get("pickupAddress").asText()).isEqualTo(PICKUP_ADDRESS);
        assertThat(body.get("dropoffAddress").asText()).isEqualTo(DROPOFF_ADDRESS);
        assertThat(body.get("createdAt").asText()).isNotBlank();
        assertThat(body.get("updatedAt").asText()).isNotBlank();
    }

    @Test
    void placedOnlyDeliveryIsPendingWithDropoffAddressAndNoDriver() {
        UUID orderId = UUID.randomUUID();
        sendPlaced(orderId, UUID.randomUUID());
        awaitDeliveryStatus(orderId, "PENDING");

        JsonNode body = get("/deliveries/" + orderId).body();
        assertThat(body.get("dropoffAddress").asText()).isEqualTo(DROPOFF_ADDRESS);
        assertThat(body.get("driverId") == null || body.get("driverId").isNull()).isTrue();
        assertThat(pickup(orderId).status()).isEqualTo(409);
        assertThat(deliver(orderId).status()).isEqualTo(409);
    }

    // ------------------------------------------------------------------ pickup and deliver

    @Test
    void pickupAndDeliverUnknownDeliveryReturn404() {
        UUID unknown = UUID.randomUUID();
        assertThat(pickup(unknown).status()).isEqualTo(404);
        assertThat(deliver(unknown).status()).isEqualTo(404);
    }

    @Test
    void pickupWhileAssignedReturns409AndChangesNothing() {
        UUID orderId = UUID.randomUUID();
        assignedOrder(orderId, UUID.randomUUID());

        Resp r = pickup(orderId);
        assertThat(r.status()).isEqualTo(409);
        assertThat(r.contentType()).contains("problem+json");
        assertThat(deliveryStatus(orderId)).isEqualTo("ASSIGNED");
        assertNoDeliveryEvent(orderId, EventTypes.ORDER_PICKED_UP);
    }

    @Test
    void deliverWhenNotPickedUpReturns409() {
        UUID orderId = UUID.randomUUID();
        EventEnvelope[] saga = assignedOrder(orderId, UUID.randomUUID());
        assertThat(deliver(orderId).status()).isEqualTo(409);

        sendReady(saga[1]);
        awaitDeliveryStatus(orderId, "READY_FOR_PICKUP");
        assertThat(deliver(orderId).status()).isEqualTo(409);
        assertThat(deliveryStatus(orderId)).isEqualTo("READY_FOR_PICKUP");
        assertNoDeliveryEvent(orderId, EventTypes.ORDER_DELIVERED);
    }

    @Test
    void fullFlowPickupThenDeliverPublishesEventsWithNullCausationAndFreesDriver() {
        UUID orderId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        EventEnvelope[] saga = assignedOrder(orderId, correlationId);
        assertThat(pickup(orderId).status()).isEqualTo(409);

        sendReady(saga[1]);
        awaitDeliveryStatus(orderId, "READY_FOR_PICKUP");

        Resp picked = pickup(orderId);
        assertThat(picked.status()).isEqualTo(200);
        assertThat(picked.body().get("status").asText()).isEqualTo("PICKED_UP");
        assertThat(picked.body().get("orderId").asText()).isEqualTo(orderId.toString());
        Published pickedUp = awaitDeliveryEvents(orderId, EventTypes.ORDER_PICKED_UP).get(0);
        assertEnvelope(pickedUp, EventTypes.ORDER_PICKED_UP, orderId, correlationId, null);
        OrderPickedUp pickedPayload = (OrderPickedUp) pickedUp.payload();
        assertThat(pickedPayload.driverId()).isEqualTo(DRIVER_1);
        assertThat(pickedPayload.pickedUpAt()).isNotNull();
        assertThat(driverStatus(DRIVER_1)).isEqualTo("BUSY");

        assertThat(pickup(orderId).status()).isEqualTo(409);

        Resp delivered = deliver(orderId);
        assertThat(delivered.status()).isEqualTo(200);
        assertThat(delivered.body().get("status").asText()).isEqualTo("DELIVERED");
        Published deliveredEvent = awaitDeliveryEvents(orderId, EventTypes.ORDER_DELIVERED).get(0);
        assertEnvelope(deliveredEvent, EventTypes.ORDER_DELIVERED, orderId, correlationId, null);
        OrderDelivered deliveredPayload = (OrderDelivered) deliveredEvent.payload();
        assertThat(deliveredPayload.driverId()).isEqualTo(DRIVER_1);
        assertThat(deliveredPayload.deliveredAt()).isNotNull();

        assertThat(driverStatus(DRIVER_1)).isEqualTo("AVAILABLE");
        assertThat(deliveryStatus(orderId)).isEqualTo("DELIVERED");
        assertThat(deliver(orderId).status()).isEqualTo(409);
        assertThat(pickup(orderId).status()).isEqualTo(409);
    }
}
