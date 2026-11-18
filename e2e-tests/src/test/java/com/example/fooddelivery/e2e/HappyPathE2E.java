package com.example.fooddelivery.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/** Criterion 6. Capacity is raised in the base class {@code @BeforeEach} and restored after. */
class HappyPathE2E extends E2ESupport {

    static final List<String> HISTORY_A = Arrays.asList("PLACED", "ACCEPTED", "DRIVER_ASSIGNED",
            "READY_FOR_PICKUP", "OUT_FOR_DELIVERY", "DELIVERED");
    static final List<String> HISTORY_B = Arrays.asList("PLACED", "ACCEPTED", "READY_FOR_PICKUP",
            "DRIVER_ASSIGNED", "OUT_FOR_DELIVERY", "DELIVERED");

    @Test
    void orderIsDeliveredAndDriverIsFreed() {
        UUID id = placeOrder(PASTA_PLACE, MARGHERITA);

        awaitOrderStatus(id, "DRIVER_ASSIGNED");
        String driverId = orderDriverId(id);
        assertThat(driverId).isNotEmpty().isNotEqualTo("null");

        assertThat(post(RESTAURANT_URL + "/tickets/" + id + "/ready", null).status()).isEqualTo(200);
        awaitOrderStatus(id, "READY_FOR_PICKUP");

        assertThat(post(DELIVERY_URL + "/deliveries/" + id + "/pickup", null).status()).isEqualTo(200);
        awaitOrderStatus(id, "OUT_FOR_DELIVERY");
        assertThat(post(DELIVERY_URL + "/deliveries/" + id + "/deliver", null).status()).isEqualTo(200);
        awaitOrderStatus(id, "DELIVERED");

        assertThat(history(id)).isIn(HISTORY_A, HISTORY_B);

        Awaitility.await().atMost(SAGA_TIMEOUT).untilAsserted(
                () -> assertThat(driverStatus(driverId)).isEqualTo("AVAILABLE"));
    }
}
