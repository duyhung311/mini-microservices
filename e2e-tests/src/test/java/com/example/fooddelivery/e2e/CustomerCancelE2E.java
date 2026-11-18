package com.example.fooddelivery.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/** Criterion 9. */
class CustomerCancelE2E extends E2ESupport {

    @Test
    void cancelInDriverAssignedCancelsEverythingAndFreesDriver() {
        UUID id = placeOrder(PASTA_PLACE, MARGHERITA);
        awaitOrderStatus(id, "DRIVER_ASSIGNED");
        String driverId = orderDriverId(id);

        assertThat(post(ORDER_URL + "/orders/" + id + "/cancel", null).status()).isEqualTo(200);

        awaitOrderStatus(id, "CANCELLED");
        Awaitility.await().atMost(SAGA_TIMEOUT).untilAsserted(() -> {
            assertThat(ticketStatus(id)).isEqualTo("CANCELLED");
            assertThat(deliveryStatus(id)).isEqualTo("CANCELLED");
            assertThat(driverStatus(driverId)).isEqualTo("AVAILABLE");
        });

        assertThat(post(ORDER_URL + "/orders/" + id + "/cancel", null).status()).isEqualTo(409);
    }

    @Test
    void cancelUnknownOrderIs404() {
        assertThat(post(ORDER_URL + "/orders/" + UUID.randomUUID() + "/cancel", null).status()).isEqualTo(404);
    }
}
