package com.example.fooddelivery.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.RejectReason;
import com.example.fooddelivery.events.Topics;
import com.example.fooddelivery.order.timeout.OrderTimeoutJob;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

/**
 * Criterion 10 (spec 7.5): PLACED orders older than orders.timeout.placed-max-age are cancelled exactly once.
 *
 * Determinism: instead of waiting for real time to pass, a controllable clock (TestClock, a @Primary Clock bean) is
 * jumped forward and OrderTimeoutJob.runOnce() is called directly. The scheduled scan is parked at PT1H so it cannot
 * race with the test (it only runs once, at start-up, when no order is old). Order.createdAt is stamped with the
 * real clock, so the age limit (default PT5M) is crossed by advancing the test clock by 10 minutes.
 */
@Import(TestClockConfig.class)
@TestPropertySource(properties = "orders.timeout.scan-interval=PT1H")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class OrderTimeoutIT extends AbstractOrderIT {

    private static final Duration JUMP = Duration.ofMinutes(10);

    private static ConfigurableApplicationContext second;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private OrderTimeoutJob job;

    @BeforeEach
    @AfterEach
    void resetClock() {
        TestClock.INSTANCE.reset();
    }

    @AfterAll
    static void closeSecond() {
        if (second != null) {
            second.close();
            second = null;
        }
    }

    private UUID place() {
        return UUID.fromString(placeOrder().get("orderId").asText());
    }

    private int count(String sql, Object... args) {
        return jdbc.queryForObject(sql, Integer.class, args);
    }

    @Test
    void placedOrderOlderThanLimitIsCancelledOnceWithTimeoutEvent() {
        UUID orderId = place();
        EventEnvelope placed = awaitPlaced(orderId);

        // not old enough yet: nothing happens
        job.runOnce();
        assertThat(status(orderId)).isEqualTo("PLACED");

        TestClock.INSTANCE.advanceTo(JUMP);
        assertThat(job.runOnce()).isGreaterThanOrEqualTo(1);

        assertThat(status(orderId)).isEqualTo("CANCELLED");
        assertThat(history(orderId)).isEqualTo(historyOf("PLACED", "CANCELLED"));
        List<Received> cancelled = awaitOrderEvents(orderId, EventTypes.ORDER_CANCELLED, 1);
        assertThat(cancelled).hasSize(1);
        Received r = cancelled.get(0);
        assertThat(r.key()).isEqualTo(orderId.toString());
        assertThat(r.envelope().orderId()).isEqualTo(orderId);
        assertThat(r.envelope().causationId()).isNull();
        assertThat(r.envelope().correlationId()).isEqualTo(placed.correlationId());
        assertThat(r.raw()).contains("TIMEOUT");
        // the history row of the transition carries that event's id
        assertThat(jdbc.queryForObject("select event_id from order_status_history where order_id = ? "
                + "and to_status = 'CANCELLED'", UUID.class, orderId)).isEqualTo(r.envelope().eventId());

        // a second scan, still past the limit, does nothing more
        job.runOnce();
        Awaitility.await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(orderEventsOfType(orderId, EventTypes.ORDER_CANCELLED)).hasSize(1);
            assertThat(history(orderId)).isEqualTo(historyOf("PLACED", "CANCELLED"));
        });
    }

    @Test
    void ordersInOtherStatesAreUntouchedPastTheLimit() {
        UUID accepted = place();
        UUID rejected = place();
        UUID assigned = place();
        UUID stalePlaced = place();
        EventEnvelope pA = awaitPlaced(accepted);
        EventEnvelope pR = awaitPlaced(rejected);
        EventEnvelope pD = awaitPlaced(assigned);
        awaitPlaced(stalePlaced);

        send(Topics.RESTAURANT_EVENTS, SagaEvents.accepted(pA));
        send(Topics.RESTAURANT_EVENTS, SagaEvents.rejected(pR, RejectReason.CLOSED));
        EventEnvelope acceptedD = SagaEvents.accepted(pD);
        send(Topics.RESTAURANT_EVENTS, acceptedD);
        send(Topics.DELIVERY_EVENTS, SagaEvents.driverAssigned(acceptedD, UUID.randomUUID()));
        awaitStatus(accepted, "ACCEPTED");
        awaitStatus(rejected, "REJECTED");
        awaitStatus(assigned, "DRIVER_ASSIGNED");

        TestClock.INSTANCE.advanceTo(JUMP);
        job.runOnce();

        // the control order proves the scan really ran past the limit
        assertThat(status(stalePlaced)).isEqualTo("CANCELLED");
        Awaitility.await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(status(accepted)).isEqualTo("ACCEPTED");
            assertThat(status(rejected)).isEqualTo("REJECTED");
            assertThat(status(assigned)).isEqualTo("DRIVER_ASSIGNED");
            assertThat(history(accepted)).isEqualTo(historyOf("PLACED", "ACCEPTED"));
            assertThat(history(rejected)).isEqualTo(historyOf("PLACED", "REJECTED"));
            assertThat(history(assigned)).isEqualTo(historyOf("PLACED", "ACCEPTED", "DRIVER_ASSIGNED"));
        });
        for (UUID id : List.of(accepted, rejected, assigned)) {
            assertThat(count("select count(*) from outbox where aggregate_id = ? and event_type = 'OrderCancelled'",
                    id)).isZero();
        }
    }

    @Test
    void lateOrderAcceptedAfterTimeoutIsStale() {
        UUID orderId = place();
        EventEnvelope placed = awaitPlaced(orderId);
        TestClock.INSTANCE.advanceTo(JUMP);
        job.runOnce();
        assertThat(status(orderId)).isEqualTo("CANCELLED");

        EventEnvelope late = SagaEvents.accepted(placed);
        send(Topics.RESTAURANT_EVENTS, late);

        // wait until the late event has been consumed (recorded as processed), then check nothing changed
        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200)).untilAsserted(
                () -> assertThat(count("select count(*) from processed_events where event_id = ?",
                        late.eventId())).isEqualTo(1));
        Awaitility.await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(status(orderId)).isEqualTo("CANCELLED");
            assertThat(history(orderId)).isEqualTo(historyOf("PLACED", "CANCELLED"));
        });
        assertThat(orderEventsOfType(orderId, EventTypes.ORDER_CANCELLED)).hasSize(1);
    }

    @Test
    void twoInstancesCancelEachTimedOutOrderExactlyOnce() throws Exception {
        if (second == null) {
            second = new SpringApplicationBuilder(OrderApplication.class, TestClockConfig.class)
                    .run("--server.port=0",
                            "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                            "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                            "--spring.datasource.username=" + POSTGRES.getUsername(),
                            "--spring.datasource.password=" + POSTGRES.getPassword(),
                            "--orders.timeout.scan-interval=PT1H");
        }
        OrderTimeoutJob jobB = second.getBean(OrderTimeoutJob.class);
        assertThat(jobB).isNotSameAs(job);

        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ids.add(place());
        }
        for (UUID id : ids) {
            awaitPlaced(id);
        }

        TestClock.INSTANCE.advanceTo(JUMP);
        CyclicBarrier barrier = new CyclicBarrier(2);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Callable<Integer> a = () -> {
                barrier.await(10, TimeUnit.SECONDS);
                return job.runOnce();
            };
            Callable<Integer> b = () -> {
                barrier.await(10, TimeUnit.SECONDS);
                return jobB.runOnce();
            };
            Future<Integer> fa = pool.submit(a);
            Future<Integer> fb = pool.submit(b);
            int total = fa.get(30, TimeUnit.SECONDS) + fb.get(30, TimeUnit.SECONDS);
            // total also counts other tests' stale PLACED orders; count only the cancels of these 10 ids (a double cancel gives >10)
            assertThat(total).isGreaterThanOrEqualTo(ids.size());
            int ownCancels = jdbc.queryForObject("select count(*) from order_status_history where to_status = 'CANCELLED' "
                    + "and order_id in (" + ids.stream().map(i -> "'" + i + "'").collect(Collectors.joining(",")) + ")",
                    Integer.class);
            assertThat(ownCancels).isEqualTo(ids.size());
        } finally {
            pool.shutdownNow();
        }

        Set<String> keys = ids.stream().map(UUID::toString).collect(Collectors.toSet());
        for (UUID id : ids) {
            assertThat(status(id)).isEqualTo("CANCELLED");
            assertThat(history(id)).isEqualTo(historyOf("PLACED", "CANCELLED"));
            assertThat(count("select count(*) from order_status_history where order_id = ? "
                    + "and from_status = 'PLACED' and to_status = 'CANCELLED'", id)).isEqualTo(1);
            assertThat(count("select count(*) from outbox where aggregate_id = ? and event_type = 'OrderCancelled'",
                    id)).isEqualTo(1);
        }

        // exactly one OrderCancelled record per order on order.events (wait for the relay, then check stability)
        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).untilAsserted(
                () -> assertThat(cancelledCounts(keys).values().stream().filter(n -> n >= 1).count())
                        .isEqualTo(10L));
        Awaitility.await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(15)).untilAsserted(
                () -> assertThat(cancelledCounts(keys).values()).allMatch(n -> n == 1));
    }

    private static Map<String, Integer> cancelledCounts(Set<String> keys) {
        Map<String, Integer> counts = new HashMap<>();
        for (String k : keys) {
            counts.put(k, 0);
        }
        for (ConsumerRecord<String, String> rec : TopicScan.read(List.of(Topics.ORDER_EVENTS), keys::contains)) {
            EventEnvelope e = EventCodec.fromJson(rec.value());
            if (EventTypes.ORDER_CANCELLED.equals(e.eventType())) {
                counts.merge(rec.key(), 1, Integer::sum);
                assertThat(e.causationId()).isNull();
            }
        }
        return counts;
    }
}
