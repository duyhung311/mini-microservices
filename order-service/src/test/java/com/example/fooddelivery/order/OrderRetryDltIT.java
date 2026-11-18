package com.example.fooddelivery.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.events.DeliveryFailureReason;
import com.example.fooddelivery.events.Envelopes;
import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventHeaders;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.OrderAccepted;
import com.example.fooddelivery.events.Topics;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Criteria 18 and 12 (order side). Retry delays are shrunk to 200 ms x2 (max 1000 ms) by this module's test
 * application.properties. Transient and exhausted failures are injected by a processed_events trigger
 * ({@link DbFaults}), poison records are produced directly.
 */
@ExtendWith(OutputCaptureExtension.class)
class OrderRetryDltIT extends AbstractOrderIT {

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private MeterRegistry meters;

    private final TopicTap tap = TopicTap.instance();

    private static String retry(String source, int hop) {
        return Topics.retry(source, Topics.ORDER_SERVICE, hop);
    }

    private static String dlt(String source) {
        return Topics.dlt(source, Topics.ORDER_SERVICE);
    }

    private int processedCount(UUID eventId) {
        return jdbc.queryForObject("select count(*) from processed_events where event_id = ?", Integer.class,
                eventId);
    }

    private double dltCounterTotal() {
        double sum = 0;
        for (Counter c : meters.find("kafka.dlt.records").counters()) {
            sum += c.count();
        }
        return sum;
    }

    private UUID placedOrder() {
        return UUID.fromString(placeOrder().get("orderId").asText());
    }

    private void assertNoRetryHops(String source, Object key) {
        for (int i = 0; i < Topics.RETRY_HOPS; i++) {
            assertThat(tap.count(retry(source, i), key)).as("retry-%d records for %s", i, key).isZero();
        }
    }

    // ------------------------------------------------------------------ transient

    @Test
    void transientFailureOnRestaurantEventIsRetriedThroughRetry0ThenApplied() {
        UUID orderId = placedOrder();
        EventEnvelope accepted = SagaEvents.accepted(awaitPlaced(orderId));

        try (DbFaults.Fault ignored = DbFaults.failProcessedEventInsert(jdbc, accepted.eventId(), 1)) {
            send(Topics.RESTAURANT_EVENTS, accepted);
            Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(50))
                    .until(() -> tap.count(retry(Topics.RESTAURANT_EVENTS, 0), orderId) >= 1);
            awaitStatus(orderId, "ACCEPTED");
        }

        assertThat(history(orderId)).isEqualTo(historyOf("PLACED", "ACCEPTED"));
        assertThat(processedCount(accepted.eventId())).isEqualTo(1);
        assertThat(tap.count(retry(Topics.RESTAURANT_EVENTS, 0), orderId)).isEqualTo(1);
        assertThat(tap.count(retry(Topics.RESTAURANT_EVENTS, 1), orderId)).isZero();
        assertThat(tap.count(dlt(Topics.RESTAURANT_EVENTS), orderId)).isZero();
    }

    @Test
    void transientFailureOnDeliveryEventIsRetriedThroughRetry0ThenApplied() {
        UUID orderId = placedOrder();
        EventEnvelope accepted = SagaEvents.accepted(awaitPlaced(orderId));
        send(Topics.RESTAURANT_EVENTS, accepted);
        awaitStatus(orderId, "ACCEPTED");
        UUID driver = UUID.randomUUID();
        EventEnvelope assigned = SagaEvents.driverAssigned(accepted, driver);

        try (DbFaults.Fault ignored = DbFaults.failProcessedEventInsert(jdbc, assigned.eventId(), 1)) {
            send(Topics.DELIVERY_EVENTS, assigned);
            Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(50))
                    .until(() -> tap.count(retry(Topics.DELIVERY_EVENTS, 0), orderId) >= 1);
            awaitStatus(orderId, "DRIVER_ASSIGNED");
        }

        assertThat(history(orderId)).isEqualTo(historyOf("PLACED", "ACCEPTED", "DRIVER_ASSIGNED"));
        assertThat(get("/orders/" + orderId).getBody().get("driverId").asText()).isEqualTo(driver.toString());
        assertThat(processedCount(assigned.eventId())).isEqualTo(1);
        assertThat(tap.count(dlt(Topics.DELIVERY_EVENTS), orderId)).isZero();
    }

    // ------------------------------------------------------------------ poison

    private static String poisonValue(String kind, UUID key) {
        switch (kind) {
            case "not-json":
                return "this-is-not-json";
            case "unknown-type":
                EventEnvelope env = Envelopes.first(EventTypes.ORDER_ACCEPTED, key, UUID.randomUUID(),
                        new OrderAccepted(UUID.randomUUID(), 15, "1 Kitchen St"));
                try {
                    ObjectNode node = (ObjectNode) JSON.readTree(EventCodec.toJson(env));
                    node.put("eventType", "NoSuchEvent");
                    return JSON.writeValueAsString(node);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            case "correlation-mismatch":
                return EventCodec.toJson(Envelopes.first(EventTypes.ORDER_ACCEPTED, key, UUID.randomUUID(),
                        new OrderAccepted(UUID.randomUUID(), 15, "1 Kitchen St")));
            default:
                throw new IllegalArgumentException(kind);
        }
    }

    private static Map<String, byte[]> poisonHeaders(String kind, String value) {
        Map<String, byte[]> h = new HashMap<>();
        if ("unknown-type".equals(kind)) {
            h.put(EventHeaders.EVENT_TYPE, "NoSuchEvent".getBytes(StandardCharsets.UTF_8));
        } else if ("correlation-mismatch".equals(kind)) {
            EventEnvelope env = EventCodec.fromJson(value);
            h.put(EventHeaders.EVENT_TYPE, env.eventType().getBytes(StandardCharsets.UTF_8));
            // a different correlationId than the envelope carries
            h.put(EventHeaders.CORRELATION_ID, UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        }
        return h;
    }

    @ParameterizedTest(name = "{0} on {1}")
    @CsvSource({
            "not-json, restaurant.events",
            "unknown-type, restaurant.events",
            "correlation-mismatch, restaurant.events",
            "not-json, delivery.events",
            "unknown-type, delivery.events",
            "correlation-mismatch, delivery.events"})
    void poisonGoesStraightToDltWithZeroRetryHops(String kind, String source) {
        UUID key = UUID.randomUUID();
        String value = poisonValue(kind, key);

        sendRaw(source, key.toString(), value, poisonHeaders(kind, value));

        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100))
                .until(() -> tap.count(dlt(source), key) >= 1);
        assertThat(tap.count(dlt(source), key)).isEqualTo(1);
        assertThat(tap.seen(dlt(source), key).get(0).value()).isEqualTo(value);
        // no retry hop may follow: wait out a full backoff cycle
        Awaitility.await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertNoRetryHops(source, key));
    }

    // ------------------------------------------------------------------ exhausted

    @ParameterizedTest(name = "exhausted on {0}")
    @ValueSource(strings = {"restaurant.events", "delivery.events"})
    void exhaustedRetriesGoThroughAllHopsToDltWithAlertAndCounter(String source, CapturedOutput output) {
        UUID orderId = placedOrder();
        EventEnvelope placed = awaitPlaced(orderId);
        EventEnvelope event = Topics.RESTAURANT_EVENTS.equals(source) ? SagaEvents.accepted(placed)
                : SagaEvents.driverAssigned(placed, UUID.randomUUID());
        double before = dltCounterTotal();

        try (DbFaults.Fault ignored = DbFaults.failProcessedEventInsert(jdbc, event.eventId(), 1_000_000L)) {
            send(source, event);
            Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100))
                    .until(() -> tap.count(dlt(source), orderId) >= 1);
        }

        for (int hop = 0; hop < Topics.RETRY_HOPS; hop++) {
            assertThat(tap.count(retry(source, hop), orderId)).as("retry-%d", hop).isEqualTo(1);
        }
        assertThat(tap.count(dlt(source), orderId)).isEqualTo(1);
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(output.getAll()).contains("ALERT").contains("key=" + orderId);
            assertThat(dltCounterTotal()).isGreaterThan(before);
        });
        assertThat(meters.find("kafka.dlt.records").counters()).isNotEmpty()
                .allSatisfy(c -> assertThat(c.getId().getTag("topic")).isNotNull());
        // nothing was applied or recorded
        assertThat(status(orderId)).isEqualTo("PLACED");
        assertThat(processedCount(event.eventId())).isZero();
    }

    // ------------------------------------------------------------------ stale (criterion 12)

    @Test
    void staleOrderReadyAfterCancelledIsNotRetriedNotDeadLetteredButRecorded() {
        UUID orderId = placedOrder();
        EventEnvelope placed = awaitPlaced(orderId);
        send(Topics.RESTAURANT_EVENTS, SagaEvents.accepted(placed));
        awaitStatus(orderId, "ACCEPTED");
        assertThat(post("/orders/" + orderId + "/cancel").getStatusCode().value()).isEqualTo(200);
        awaitStatus(orderId, "CANCELLED");
        java.util.List<String> before = history(orderId);

        EventEnvelope ready = SagaEvents.ready(placed);
        send(Topics.RESTAURANT_EVENTS, ready);

        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> processedCount(ready.eventId()) == 1);
        Awaitility.await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertNoRetryHops(Topics.RESTAURANT_EVENTS, orderId);
            assertThat(tap.count(dlt(Topics.RESTAURANT_EVENTS), orderId)).isZero();
        });
        assertThat(history(orderId)).isEqualTo(before);
        assertThat(status(orderId)).isEqualTo("CANCELLED");
    }

    @Test
    void staleOrderReadyAfterDeliveredIsNotRetriedNotDeadLetteredButRecorded() {
        UUID orderId = placedOrder();
        EventEnvelope placed = awaitPlaced(orderId);
        UUID driver = UUID.randomUUID();
        EventEnvelope accepted = SagaEvents.accepted(placed);
        send(Topics.RESTAURANT_EVENTS, accepted);
        awaitStatus(orderId, "ACCEPTED");
        EventEnvelope assigned = SagaEvents.driverAssigned(accepted, driver);
        send(Topics.DELIVERY_EVENTS, assigned);
        awaitStatus(orderId, "DRIVER_ASSIGNED");
        EventEnvelope ready = SagaEvents.ready(placed);
        send(Topics.RESTAURANT_EVENTS, ready);
        awaitStatus(orderId, "READY_FOR_PICKUP");
        EventEnvelope pickedUp = SagaEvents.pickedUp(ready, driver);
        send(Topics.DELIVERY_EVENTS, pickedUp);
        awaitStatus(orderId, "OUT_FOR_DELIVERY");
        send(Topics.DELIVERY_EVENTS, SagaEvents.delivered(pickedUp, driver));
        awaitStatus(orderId, "DELIVERED");
        java.util.List<String> before = history(orderId);

        EventEnvelope lateReady = SagaEvents.ready(placed);
        send(Topics.RESTAURANT_EVENTS, lateReady);

        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> processedCount(lateReady.eventId()) == 1);
        Awaitility.await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertNoRetryHops(Topics.RESTAURANT_EVENTS, orderId);
            assertNoRetryHops(Topics.DELIVERY_EVENTS, orderId);
            assertThat(tap.count(dlt(Topics.RESTAURANT_EVENTS), orderId)).isZero();
            assertThat(tap.count(dlt(Topics.DELIVERY_EVENTS), orderId)).isZero();
        });
        assertThat(history(orderId)).isEqualTo(before);
        assertThat(status(orderId)).isEqualTo("DELIVERED");
        // an applied DeliveryFailed after the terminal state is stale too
        EventEnvelope lateFailure = SagaEvents.deliveryFailed(pickedUp, DeliveryFailureReason.DRIVER_CANCELLED);
        send(Topics.DELIVERY_EVENTS, lateFailure);
        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> processedCount(lateFailure.eventId()) == 1);
        assertNoRetryHops(Topics.DELIVERY_EVENTS, orderId);
        assertThat(tap.count(dlt(Topics.DELIVERY_EVENTS), orderId)).isZero();
    }
}
