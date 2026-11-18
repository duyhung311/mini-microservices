package com.example.fooddelivery.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.delivery.support.DeliveryIntegrationTestBase;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventTypes;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

/**
 * Criterion 20, auto-advance disabled (the default, set explicitly here). The delays and tick are made short
 * so that, if the scheduler ran, the delivery would certainly have moved within the 3 second quiet period.
 */
@TestPropertySource(properties = {
        "simulation.auto-advance=false",
        "simulation.pickup-delay=PT1S",
        "simulation.delivery-delay=PT1S",
        "simulation.tick=PT0.5S"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class DeliveryAutoAdvanceDisabledIT extends DeliveryIntegrationTestBase {

    @Test
    void nothingAdvancesWhenAutoAdvanceIsFalse() {
        UUID orderId = UUID.randomUUID();
        EventEnvelope[] saga = assignedOrder(orderId, UUID.randomUUID());
        sendReady(saga[1]);
        awaitDeliveryStatus(orderId, "READY_FOR_PICKUP");

        assertDeliveryStaysAt(orderId, "READY_FOR_PICKUP");
        assertNoDeliveryEvent(orderId, EventTypes.ORDER_PICKED_UP);
        assertNoDeliveryEvent(orderId, EventTypes.ORDER_DELIVERED);
        assertThat(driverStatus(DRIVER_1)).isEqualTo("BUSY");
    }
}
