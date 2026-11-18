package com.example.fooddelivery.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.delivery.support.DeliveryIntegrationTestBase;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.OrderDelivered;
import com.example.fooddelivery.events.OrderPickedUp;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

/** Criterion 20, auto-advance enabled: a READY_FOR_PICKUP delivery reaches PICKED_UP then DELIVERED by itself. */
@TestPropertySource(properties = {
        "simulation.auto-advance=true",
        "simulation.pickup-delay=PT1S",
        "simulation.delivery-delay=PT1S",
        "simulation.tick=PT0.5S"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class DeliveryAutoAdvanceIT extends DeliveryIntegrationTestBase {

    @Test
    void readyForPickupAdvancesToPickedUpThenDeliveredWithoutRestCalls() {
        UUID orderId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        EventEnvelope[] saga = assignedOrder(orderId, correlationId);
        sendReady(saga[1]);

        Published pickedUp = awaitDeliveryEvents(orderId, EventTypes.ORDER_PICKED_UP).get(0);
        assertEnvelope(pickedUp, EventTypes.ORDER_PICKED_UP, orderId, correlationId, null);
        assertThat(((OrderPickedUp) pickedUp.payload()).driverId()).isEqualTo(DRIVER_1);

        Published delivered = awaitDeliveryEvents(orderId, EventTypes.ORDER_DELIVERED).get(0);
        assertEnvelope(delivered, EventTypes.ORDER_DELIVERED, orderId, correlationId, null);
        assertThat(((OrderDelivered) delivered.payload()).driverId()).isEqualTo(DRIVER_1);

        assertThat(pickedUp.envelope().occurredAt()).isBeforeOrEqualTo(delivered.envelope().occurredAt());
        awaitDeliveryStatus(orderId, "DELIVERED");
        awaitDriverStatus(DRIVER_1, "AVAILABLE");
        assertThat(deliveryEvents(orderId, EventTypes.ORDER_PICKED_UP)).hasSize(1);
        assertThat(deliveryEvents(orderId, EventTypes.ORDER_DELIVERED)).hasSize(1);
    }

    @Test
    void assignedDeliveryWithoutReadyIsNotPickedUp() {
        UUID orderId = UUID.randomUUID();
        assignedOrder(orderId, UUID.randomUUID());

        assertDeliveryStaysAt(orderId, "ASSIGNED");
        assertNoDeliveryEvent(orderId, EventTypes.ORDER_PICKED_UP);
    }
}
