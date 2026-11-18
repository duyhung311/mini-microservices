package com.example.fooddelivery.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.UUID;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/** Criterion 7: three reject reasons, each ending REJECTED with no OrderCancelled on order.events. */
class RestaurantRejectsE2E extends E2ESupport {

    private void assertRejectedWithoutCancel(String restaurantId, String itemId) {
        try (EventTap tap = new EventTap("order.events")) {
            UUID id = placeOrder(restaurantId, itemId);
            awaitOrderStatus(id, "REJECTED");
            Awaitility.await().atMost(Duration.ofSeconds(30)).pollInSameThread().untilAsserted(
                    () -> assertThat(tap.typesFor(id)).contains("OrderPlaced"));
            Awaitility.await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(15))
                    .pollInSameThread().pollInterval(Duration.ofMillis(300))
                    .untilAsserted(() -> {
                        assertThat(tap.typesFor(id)).doesNotContain("OrderCancelled");
                        assertThat(orderStatus(id)).isEqualTo("REJECTED");
                    });
        }
    }

    @Test
    void closedRestaurantIsRejected() {
        assertRejectedWithoutCancel(NIGHT_GRILL, MARGHERITA);
    }

    @Test
    void unavailableItemIsRejected() {
        assertRejectedWithoutCancel(PASTA_PLACE, TRUFFLE);
    }

    @Test
    void atCapacityIsRejected() {
        try {
            patchRestaurant(PASTA_PLACE, "{\"maxConcurrentOrders\":0}");
            assertRejectedWithoutCancel(PASTA_PLACE, MARGHERITA);
        } finally {
            patchRestaurant(PASTA_PLACE, "{\"maxConcurrentOrders\":" + SEEDED_CAPACITY + "}");
        }
    }
}
