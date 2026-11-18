package com.example.fooddelivery.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.events.EventEnvelope;
import java.time.Duration;
import java.util.UUID;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/** Criterion 8: all drivers OFFLINE gives a CANCELLED order and ticket and OrderCancelled(NO_DRIVER). */
class NoDriverE2E extends E2ESupport {

    @Test
    void noDriverCancelsOrderAndTicket() {
        try (EventTap tap = new EventTap("order.events")) {
            try {
                for (int i = 1; i <= DRIVER_COUNT; i++) {
                    assertThat(patchDriver(driverId(i), "OFFLINE").status()).isEqualTo(200);
                }
                UUID id = placeOrder(PASTA_PLACE, MARGHERITA);

                awaitOrderStatus(id, "CANCELLED");
                Awaitility.await().atMost(SAGA_TIMEOUT).untilAsserted(
                        () -> assertThat(ticketStatus(id)).isEqualTo("CANCELLED"));

                Awaitility.await().atMost(Duration.ofSeconds(30)).pollInSameThread().untilAsserted(() -> {
                    EventEnvelope cancelled = tap.eventsFor(id).stream()
                            .filter(e -> "OrderCancelled".equals(e.eventType())).findFirst().orElse(null);
                    assertThat(cancelled).as("OrderCancelled on order.events").isNotNull();
                    assertThat(cancelled.payload().get("reason").asText()).isEqualTo("NO_DRIVER");
                });
            } finally {
                for (int i = 1; i <= DRIVER_COUNT; i++) {
                    patchDriver(driverId(i), "AVAILABLE");
                }
            }
        }
    }
}
