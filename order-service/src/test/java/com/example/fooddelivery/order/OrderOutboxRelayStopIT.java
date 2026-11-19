package com.example.fooddelivery.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.events.CancelReason;
import com.example.fooddelivery.events.Envelopes;
import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.OrderCancelled;
import com.example.fooddelivery.events.Topics;
import com.example.fooddelivery.order.outbox.OutboxRelay;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Criterion 15: the relay stops the batch at the first failing row (head-of-line blocking). R1 is valid, R2 is valid
 * JSON that fails envelope decoding, R3 is valid. With a "continue" instead of a "return" R3 would be published.
 * All three rows share one aggregate id, so they share a partition and the topic order is the id order.
 */
class OrderOutboxRelayStopIT extends AbstractOrderIT {

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private PlatformTransactionManager txManager;

    @Autowired
    private OutboxRelay relay;

    private static EventEnvelope cancelled(UUID aggregate) {
        return Envelopes.first(EventTypes.ORDER_CANCELLED, aggregate, UUID.randomUUID(),
                new OrderCancelled(CancelReason.CUSTOMER_REQUEST));
    }

    private long insert(UUID aggregate, String payload) {
        return jdbc.queryForObject("insert into outbox (aggregate_id, event_type, payload) "
                + "values (?, 'OrderCancelled', ?::jsonb) returning id", Long.class, aggregate, payload);
    }

    private boolean published(long id) {
        return Boolean.TRUE.equals(jdbc.queryForObject("select published_at is not null from outbox where id = ?",
                Boolean.class, id));
    }

    /**
     * Runs {@code mutation} and then the relay inside ONE transaction: the uncommitted changes are invisible to the
     * relays of other cached test contexts, so this context's relay call is the only one that sees them.
     */
    private void inOneTxRelayUntil(Runnable mutation, BooleanSupplier done) {
        new TransactionTemplate(txManager).executeWithoutResult(tx -> {
            mutation.run();
            // pollInSameThread: relay() joins this transaction; retry while another relay holds the advisory lock
            Awaitility.await().pollInSameThread().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(100))
                    .until(() -> {
                relay.relay();
                return done.getAsBoolean();
            });
        });
    }

    @Test
    void relayStopsTheBatchAtTheFirstFailingRowAndResumesInIdOrderOnceItIsFixed() {
        UUID aggregate = UUID.randomUUID();
        EventEnvelope e1 = cancelled(aggregate);
        EventEnvelope e2 = cancelled(aggregate);
        EventEnvelope e3 = cancelled(aggregate);
        long[] ids = new long[3];
        try {
            // step 1+2: insert R1, R2 (valid JSON, not an envelope), R3 and run the relay; the commit makes R1 durable
            inOneTxRelayUntil(() -> {
                ids[0] = insert(aggregate, EventCodec.toJson(e1));
                ids[1] = insert(aggregate, "{}");
                ids[2] = insert(aggregate, EventCodec.toJson(e3));
            }, () -> published(ids[0]));

            // step 3: R1 published, R2 and R3 not; only R1 is on the topic
            assertThat(published(ids[0])).isTrue();
            assertThat(published(ids[1])).as("R2 (poisoned)").isFalse();
            assertThat(published(ids[2])).as("R3 behind the failing row").isFalse();
            Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(300)).until(
                    () -> readTopic(Topics.ORDER_EVENTS, aggregate.toString()).size() >= 1);
            List<Received> afterStop = readTopic(Topics.ORDER_EVENTS, aggregate.toString());
            assertThat(afterStop).extracting(r -> r.envelope().eventId()).containsExactly(e1.eventId());

            // step 4+5: repair R2 and COMMIT it, then let any relay publish; R2 and R3 follow in id order.
            // (Repairing inside this test's transaction would hold R2's row lock: another context's relay that holds
            // the advisory lock then blocks on FOR UPDATE, and this context's relay never gets the lock.)
            jdbc.update("update outbox set payload = ?::jsonb where id = ?", EventCodec.toJson(e2), ids[1]);
            Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200)).until(() -> {
                relay.relay();
                return published(ids[1]) && published(ids[2]);
            });
            Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(300)).until(
                    () -> readTopic(Topics.ORDER_EVENTS, aggregate.toString()).size() >= 3);
            List<Received> all = readTopic(Topics.ORDER_EVENTS, aggregate.toString());
            assertThat(all).extracting(r -> r.envelope().eventId())
                    .containsExactly(e1.eventId(), e2.eventId(), e3.eventId());
        } finally {
            // a poisoned row would block every later outbox row of this service: never leave it behind
            jdbc.update("delete from outbox where aggregate_id = ? and published_at is null", aggregate);
        }
    }
}
