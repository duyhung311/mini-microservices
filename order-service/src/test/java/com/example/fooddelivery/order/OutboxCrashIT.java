package com.example.fooddelivery.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Criterion 15 (crash): with the Kafka container paused, POST /orders still returns 202 and the outbox row stays
 * unpublished for at least 5 s. After unpause the record arrives within 60 s and published_at is set.
 * Own class: the paused container is the shared singleton. With the producer's max.block.ms=5000 and
 * request.timeout.ms=10000 each relay attempt fails within about 10 s.
 */
class OutboxCrashIT extends AbstractOrderIT {

    @Autowired
    private JdbcTemplate jdbc;

    private int count(String sql, Object... args) {
        return jdbc.queryForObject(sql, Integer.class, args);
    }

    @Test
    void orderIsAcceptedWhileKafkaIsDownAndPublishedAfterItComesBack() {
        UUID orderId;
        boolean paused = false;
        try {
            KafkaPause.pause();
            paused = true;

            JsonNode body = placeOrder(); // asserts 202
            orderId = UUID.fromString(body.get("orderId").asText());
            assertThat(body.get("status").asText()).isEqualTo("PLACED");
            assertThat(count("select count(*) from outbox where aggregate_id = ? and event_type = 'OrderPlaced'",
                    orderId)).isEqualTo(1);

            // stays unpublished for at least 5 s (the relay keeps failing against the paused broker)
            final UUID id = orderId;
            Awaitility.await().during(Duration.ofSeconds(5)).atMost(Duration.ofSeconds(20))
                    .pollInterval(Duration.ofMillis(500))
                    .until(() -> count("select count(*) from outbox where aggregate_id = ? and published_at is null",
                            id) == 1);
            assertThat(status(orderId)).isEqualTo("PLACED");
        } finally {
            if (paused) {
                KafkaPause.unpauseQuietly();
            }
        }

        final UUID id = orderId;
        Awaitility.await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(500))
                .until(() -> count("select count(*) from outbox where aggregate_id = ? and published_at is not null",
                        id) == 1);
        List<Received> records = awaitOrderEvents(orderId, "OrderPlaced", 1);
        assertThat(records.get(0).key()).isEqualTo(orderId.toString());
        assertThat(count("select count(*) from outbox where aggregate_id = ? and published_at is null", id)).isZero();
    }
}
