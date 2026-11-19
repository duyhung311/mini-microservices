package com.example.fooddelivery.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.events.Topics;
import com.example.fooddelivery.order.outbox.OutboxRelay;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Criterion 30 (outbox part): the traceparent is stored with the row, and the relay continues it in a child span.
 * {@code @SpringBootTest} disables tracing by default, so a real tracer is switched on for this class; its own
 * context is closed afterwards so it does not linger in the order-service consumer group.
 */
@AutoConfigureObservability(metrics = false)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class OutboxTraceparentIT extends AbstractOrderIT {

    private static final Pattern TRACEPARENT = Pattern.compile("^00-([0-9a-f]{32})-([0-9a-f]{16})-01$");

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private PlatformTransactionManager txManager;

    @Autowired
    private OutboxRelay relay;

    private static Matcher parse(String traceparent) {
        assertThat(traceparent).isNotNull();
        Matcher m = TRACEPARENT.matcher(traceparent);
        assertThat(m.matches()).as("traceparent format: %s", traceparent).isTrue();
        return m;
    }

    @Test
    void postedOrderStoresTraceparentWithCorrelationIdAsTraceId() {
        com.fasterxml.jackson.databind.JsonNode body = placeOrder();
        UUID orderId = UUID.fromString(body.get("orderId").asText());
        String hex = body.get("correlationId").asText().replace("-", "");

        List<Map<String, Object>> rows = jdbc.queryForList(
                "select traceparent from outbox where aggregate_id = ? and event_type = 'OrderPlaced'", orderId);
        assertThat(rows).hasSize(1);
        Matcher m = parse((String) rows.get(0).get("traceparent"));
        assertThat(m.group(1)).isEqualTo(hex);
    }

    @Test
    void relayPublishesInChildSpanOfStoredTraceparent() {
        // borrow a valid envelope payload from a real order, then store it under a fresh aggregate id
        UUID source = UUID.fromString(placeOrder().get("orderId").asText());
        String payload = jdbc.queryForObject(
                "select payload::text from outbox where aggregate_id = ? and event_type = 'OrderPlaced'",
                String.class, source);

        UUID aggregate = UUID.randomUUID();
        String storedTrace = UUID.randomUUID().toString().replace("-", "");
        String storedSpan = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        String stored = "00-" + storedTrace + "-" + storedSpan + "-01";
        // Insert and relay in ONE transaction: the uncommitted row is invisible to the relays of other cached
        // test contexts (which run with tracing disabled), so this context's traced relay is the one that
        // publishes it. relay() joins this transaction; retry while another relay holds the advisory lock.
        new TransactionTemplate(txManager).executeWithoutResult(tx -> {
            jdbc.update("insert into outbox (aggregate_id, event_type, payload, traceparent) "
                    + "values (?, 'OrderPlaced', ?::jsonb, ?)", aggregate, payload, stored);
            // pollInSameThread: the condition must run on this thread to see the uncommitted row.
            Awaitility.await().pollInSameThread().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(100))
                    .until(() -> {
                relay.relay();
                return jdbc.queryForObject("select published_at is not null from outbox where aggregate_id = ?",
                        Boolean.class, aggregate);
            });
        });

        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(300)).until(
                () -> !readTopic(Topics.ORDER_EVENTS, aggregate.toString()).isEmpty());

        List<Received> records = readTopic(Topics.ORDER_EVENTS, aggregate.toString());
        String header = records.get(0).headers().get("traceparent");
        Matcher m = parse(header);
        assertThat(m.group(1)).as("same traceId as the stored traceparent").isEqualTo(storedTrace);
        assertThat(m.group(2)).as("child span, not the stored parent span").isNotEqualTo(storedSpan);
    }
}
