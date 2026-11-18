package com.example.fooddelivery.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.OrderPlaced;
import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.common.utils.Utils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/** Criteria 3 and 4 (order side): key, headers, envelope and partitioning of OrderPlaced. */
class OrderPlacementIT extends AbstractOrderIT {

    @Test
    void exactlyOneOrderPlacedWithKeyEqualToOrderIdAndEnvelopeOrderId() {
        UUID customerId = UUID.randomUUID();
        UUID restaurantId = UUID.randomUUID();
        UUID menuItemId = UUID.randomUUID();
        String items = "[" + itemJson(menuItemId, 2, "12.50") + "]";
        JsonNode placed = postRaw("/orders", orderJson(customerId, restaurantId, items, "9 Elm Rd")).getBody();
        UUID orderId = UUID.fromString(placed.get("orderId").asText());

        List<Received> placedEvents = awaitOrderEvents(orderId, "OrderPlaced", 1);

        // a second read after the first confirms there is no duplicate
        Awaitility.await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(10))
                .until(() -> orderEventsOfType(orderId, "OrderPlaced").size() == 1);
        assertThat(placedEvents).hasSize(1);
        Received rec = placedEvents.get(0);
        EventEnvelope env = rec.envelope();

        assertThat(rec.key()).isEqualTo(orderId.toString());
        assertThat(env.orderId()).isEqualTo(orderId);
        assertThat(env.eventType()).isEqualTo("OrderPlaced");
        assertThat(env.eventVersion()).isEqualTo(1);
        assertThat(env.eventId()).isNotNull();
        assertThat(env.occurredAt()).isNotNull();
        assertThat(env.correlationId().toString()).isEqualTo(placed.get("correlationId").asText());
        assertThat(env.causationId()).as("REST-originated event has no cause").isNull();

        // Kafka headers equal the envelope values
        assertThat(rec.headers().get("eventType")).isEqualTo(env.eventType());
        assertThat(rec.headers().get("eventVersion")).isEqualTo(String.valueOf(env.eventVersion()));
        assertThat(rec.headers().get("correlationId")).isEqualTo(env.correlationId().toString());

        // payload matches the request
        OrderPlaced payload = (OrderPlaced) EventCodec.payloadOf(env);
        assertThat(payload.customerId()).isEqualTo(customerId);
        assertThat(payload.restaurantId()).isEqualTo(restaurantId);
        assertThat(payload.deliveryAddress()).isEqualTo("9 Elm Rd");
        assertThat(payload.items()).hasSize(1);
        assertThat(payload.items().get(0).menuItemId()).isEqualTo(menuItemId);
        assertThat(payload.items().get(0).quantity()).isEqualTo(2);
        assertThat(payload.items().get(0).unitPrice()).isEqualByComparingTo(new BigDecimal("12.50"));
        assertThat(payload.totalAmount()).isEqualByComparingTo(new BigDecimal("25.00"));
    }

    @Test
    void causationIdIsPresentAndNullInTheRawJson() {
        UUID orderId = UUID.fromString(placeOrder().get("orderId").asText());
        String raw = awaitOrderEvents(orderId, "OrderPlaced", 1).get(0).raw();
        assertThat(raw).contains("\"causationId\":null");
    }

    @Test
    void recordsOfOneOrderSitOnOnePartitionAndMatchKeyHash() {
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            ids.add(UUID.fromString(placeOrder().get("orderId").asText()));
        }
        // add a second record per order on the same topic: a customer cancel
        for (UUID id : ids) {
            assertThat(post("/orders/" + id + "/cancel").getStatusCode().value()).isEqualTo(200);
        }

        for (UUID id : ids) {
            List<Received> all = new ArrayList<>();
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> {
                all.clear();
                all.addAll(orderEventsFor(id));
                return all.size() >= 2;
            });
            assertThat(all.stream().map(Received::type)).contains("OrderPlaced", "OrderCancelled");
            assertThat(partitionsOf(all)).as("partitions of order %s", id).hasSize(1);
            int expectedPartition = Utils.toPositive(Utils.murmur2(id.toString().getBytes(StandardCharsets.UTF_8)))
                    % 3;
            assertThat(all.get(0).partition()).isEqualTo(expectedPartition);
            all.forEach(r -> assertThat(r.key()).isEqualTo(id.toString()));
        }
    }
}
