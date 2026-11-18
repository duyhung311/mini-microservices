package com.example.fooddelivery.order;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Criterion 15 (relay): rows are published in id order and the batch stops at the first failure. Kafka is paused so
 * that sends fail, then unpaused. Own class because it pauses the shared Kafka container. Delivery is at-least-once,
 * so duplicates of a record are tolerated; the assertions are about first occurrences and the published prefix.
 */
class OrderOutboxRelayIT extends AbstractOrderIT {

    @Autowired
    private JdbcTemplate jdbc;

    private List<Map<String, Object>> rowsOf(List<UUID> orders) {
        String in = orders.stream().map(o -> "'" + o + "'").collect(Collectors.joining(","));
        return jdbc.queryForList("select id, aggregate_id, event_type, published_at from outbox "
                + "where aggregate_id in (" + in + ") order by id");
    }

    @Test
    void relayPublishesInIdOrderAndStopsAtFirstFailure() {
        List<UUID> orders = new ArrayList<>();
        boolean paused = false;
        try {
            KafkaPause.pause();
            paused = true;
            for (int i = 0; i < 3; i++) {
                UUID id = UUID.fromString(placeOrder().get("orderId").asText());
                assertThat(post("/orders/" + id + "/cancel").getStatusCode().value()).isEqualTo(200);
                orders.add(id);
            }
            // 6 rows: OrderPlaced, OrderCancelled per order, in id order
            List<Map<String, Object>> rows = rowsOf(orders);
            assertThat(rows).hasSize(6);

            // while Kafka is down nothing may be published, and no later row may overtake the first one
            Awaitility.await().during(Duration.ofSeconds(6)).atMost(Duration.ofSeconds(20))
                    .pollInterval(Duration.ofMillis(500)).untilAsserted(() -> assertThat(
                            rowsOf(orders).stream().filter(r -> r.get("published_at") != null).count()).isZero());
        } finally {
            if (paused) {
                KafkaPause.unpauseQuietly();
            }
        }

        // after unpause every poll must show a published prefix (stop at first failure, id order), until all are out
        Awaitility.await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(100)).until(() -> {
            List<Map<String, Object>> rows = rowsOf(orders);
            boolean seenUnpublished = false;
            for (Map<String, Object> r : rows) {
                boolean published = r.get("published_at") != null;
                if (seenUnpublished) {
                    assertThat(published).as("row %s published after an earlier unpublished row", r.get("id"))
                            .isFalse();
                }
                if (!published) {
                    seenUnpublished = true;
                }
            }
            return !seenUnpublished;
        });

        // published_at never decreases along the id order (a batch shares one transaction timestamp)
        List<Map<String, Object>> rows = rowsOf(orders);
        Timestamp last = null;
        for (Map<String, Object> r : rows) {
            Timestamp t = (Timestamp) r.get("published_at");
            if (last != null) {
                assertThat(t).isAfterOrEqualTo(last);
            }
            last = t;
        }

        // per key (same partition) OrderPlaced reaches Kafka before OrderCancelled, as their ids ordered them
        for (UUID id : orders) {
            List<String> types = awaitOrderEvents(id, "OrderCancelled", 1).isEmpty() ? List.of()
                    : orderEventsFor(id).stream().map(Received::type).collect(Collectors.toList());
            assertThat(types.indexOf("OrderPlaced")).as("types for %s: %s", id, types).isGreaterThanOrEqualTo(0);
            assertThat(types.indexOf("OrderPlaced")).isLessThan(types.indexOf("OrderCancelled"));
        }
    }
}
