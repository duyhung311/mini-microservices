package com.example.fooddelivery.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.springframework.test.util.AopTestUtils.getUltimateTargetObject;

import com.example.fooddelivery.delivery.idempotency.ProcessedEvents;
import com.example.fooddelivery.delivery.support.DeliveryIntegrationTestBase;
import com.example.fooddelivery.delivery.support.KafkaProbe;
import com.example.fooddelivery.delivery.support.LiveContexts;
import com.example.fooddelivery.events.CancelReason;
import com.example.fooddelivery.events.Envelopes;
import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventHeaders;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.OrderAccepted;
import com.example.fooddelivery.events.OrderCancelled;
import com.example.fooddelivery.events.OrderReady;
import com.example.fooddelivery.events.Topics;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

/**
 * Criterion 18: a transient failure is retried through .retry-0 and then succeeds; poison messages go straight to
 * the DLT; exhausted retries end on the DLT with an ALERT log and the kafka.dlt.records counter.
 *
 * <p>Cached Spring contexts of other test classes consume the same group and have no test hook, so this class
 * stops their listener containers for its duration (see {@link LiveContexts}).
 */
@TestPropertySource(properties = {
        "app.kafka.retry.delay-ms=200",
        "app.kafka.retry.multiplier=2",
        "app.kafka.retry.max-delay-ms=1000"})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(OutputCaptureExtension.class)
class DeliveryRetryDltIT extends DeliveryIntegrationTestBase {

    private static final String GROUP = Topics.DELIVERY_SERVICE;
    /** 2 main x 3 partitions + 6 retry x 3 + 2 DLT x 1 */
    private static final int EXPECTED_PARTITIONS = 26;

    @MockitoSpyBean
    ProcessedEvents processedEvents;

    @Autowired
    MeterRegistry meterRegistry;

    @Autowired
    ApplicationContext context;

    @Autowired
    KafkaListenerEndpointRegistry registry;

    @Value("${spring.kafka.bootstrap-servers}")
    String bootstrap;

    private KafkaProbe probe;
    private List<KafkaListenerEndpointRegistry> stoppedOthers;

    @BeforeAll
    void isolateAndWaitForAssignment() {
        probe = new KafkaProbe(bootstrap);
        stoppedOthers = LiveContexts.stopOthers(context);
        await().atMost(WAIT).pollInterval(POLL).until(() -> {
            int assigned = 0;
            for (MessageListenerContainer c : registry.getListenerContainers()) {
                assigned += c.getAssignedPartitions() == null ? 0 : c.getAssignedPartitions().size();
            }
            return assigned == EXPECTED_PARTITIONS;
        });
        warmUp();
    }

    /** Proves both main listeners are consuming before any test relies on timing. */
    private void warmUp() {
        EventEnvelope viaOrderTopic = sendPlaced(UUID.randomUUID(), UUID.randomUUID());
        EventEnvelope viaRestaurantTopic = Envelopes.first(EventTypes.ORDER_READY, UUID.randomUUID(),
                UUID.randomUUID(), new OrderReady(RESTAURANT, Instant.now()));
        publish(Topics.RESTAURANT_EVENTS, viaRestaurantTopic);
        await().atMost(WAIT).pollInterval(POLL).until(() ->
                count("SELECT count(*) FROM processed_events WHERE event_id IN (?, ?)",
                        viaOrderTopic.eventId(), viaRestaurantTopic.eventId()) == 2);
    }

    @AfterAll
    void restoreOthers() {
        LiveContexts.start(stoppedOthers);
    }

    /** The injected bean is the transactional proxy; stubbing must hit the Mockito spy underneath it. */
    private ProcessedEvents spy() {
        return (ProcessedEvents) getUltimateTargetObject(processedEvents);
    }

    private static String retryTopic(String source, int i) {
        return source + "." + GROUP + ".retry-" + i;
    }

    private static String dltTopic(String source) {
        return source + "." + GROUP + ".dlt";
    }

    private List<KafkaProbe.Rec> read(String topic, UUID orderId) {
        return probe.read(topic, orderId.toString());
    }

    private void awaitRecord(String topic, UUID orderId) {
        await().atMost(WAIT).pollInterval(POLL).until(() -> !read(topic, orderId).isEmpty());
    }

    private void assertNoRetryRecords(String source, UUID orderId) {
        await().during(QUIET).atMost(WAIT).pollInterval(POLL).until(() ->
                read(retryTopic(source, 0), orderId).isEmpty()
                        && read(retryTopic(source, 1), orderId).isEmpty()
                        && read(retryTopic(source, 2), orderId).isEmpty());
    }

    private long count(String sql, Object... args) {
        Long n = jdbc.queryForObject(sql, Long.class, args);
        return n == null ? 0 : n;
    }

    private double dltCounter(String topic) {
        Counter c = meterRegistry.find("kafka.dlt.records").tag("topic", topic).counter();
        return c == null ? 0 : c.count();
    }

    // ------------------------------------------------------------------ transient

    @Test
    void transientFailureOnRestaurantEventsIsRetriedThenSucceeds() {
        UUID orderId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        EventEnvelope placed = sendPlaced(orderId, correlationId);
        EventEnvelope accepted = Envelopes.next(placed, EventTypes.ORDER_ACCEPTED,
                new OrderAccepted(RESTAURANT, 15, PICKUP_ADDRESS));
        doThrow(new RuntimeException("transient")).doCallRealMethod()
                .when(spy()).firstTime(accepted.eventId());
        publish(Topics.RESTAURANT_EVENTS, accepted);

        awaitRecord(retryTopic(Topics.RESTAURANT_EVENTS, 0), orderId);
        awaitDeliveryStatus(orderId, "ASSIGNED");
        Published record = awaitDeliveryEvents(orderId, EventTypes.DRIVER_ASSIGNED).get(0);
        assertEnvelope(record, EventTypes.DRIVER_ASSIGNED, orderId, correlationId, accepted.eventId());

        verify(spy(), atLeast(2)).firstTime(accepted.eventId());
        assertThat(read(retryTopic(Topics.RESTAURANT_EVENTS, 1), orderId)).isEmpty();
        assertThat(read(dltTopic(Topics.RESTAURANT_EVENTS), orderId)).isEmpty();
        assertThat(count("SELECT count(*) FROM processed_events WHERE event_id = ?", accepted.eventId()))
                .isEqualTo(1);
        assertThat(count("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND event_type = ?", orderId,
                EventTypes.DRIVER_ASSIGNED)).isEqualTo(1);
    }

    @Test
    void transientFailureOnOrderEventsIsRetriedThenSucceeds() {
        UUID orderId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        EventEnvelope[] sent = assignedOrder(orderId, correlationId);
        EventEnvelope cancelled = Envelopes.next(sent[0], EventTypes.ORDER_CANCELLED,
                new OrderCancelled(CancelReason.CUSTOMER_REQUEST));
        doThrow(new RuntimeException("transient")).doCallRealMethod()
                .when(spy()).firstTime(cancelled.eventId());
        publish(Topics.ORDER_EVENTS, cancelled);

        awaitRecord(retryTopic(Topics.ORDER_EVENTS, 0), orderId);
        awaitDeliveryStatus(orderId, "CANCELLED");
        awaitDriverStatus(DRIVER_1, "AVAILABLE");
        assertThat(read(dltTopic(Topics.ORDER_EVENTS), orderId)).isEmpty();
        assertThat(count("SELECT count(*) FROM processed_events WHERE event_id = ?", cancelled.eventId()))
                .isEqualTo(1);
    }

    // ------------------------------------------------------------------ poison

    private EventEnvelope validEnvelope(String source, UUID orderId, UUID correlationId) {
        if (Topics.ORDER_EVENTS.equals(source)) {
            return Envelopes.first(EventTypes.ORDER_CANCELLED, orderId, correlationId,
                    new OrderCancelled(CancelReason.CUSTOMER_REQUEST));
        }
        return Envelopes.first(EventTypes.ORDER_READY, orderId, correlationId, new OrderReady(RESTAURANT,
                Instant.now()));
    }

    private void assertPoisonGoesStraightToDlt(String source, UUID orderId, double counterBefore) {
        String dlt = dltTopic(source);
        awaitRecord(dlt, orderId);
        assertThat(read(dlt, orderId)).hasSize(1);
        assertNoRetryRecords(source, orderId);
        await().atMost(WAIT).pollInterval(POLL).until(() -> dltCounter(dlt) >= counterBefore + 1);
        assertThat(count("SELECT count(*) FROM deliveries WHERE order_id = ?", orderId)).isZero();
    }

    @ParameterizedTest
    @ValueSource(strings = {Topics.ORDER_EVENTS, Topics.RESTAURANT_EVENTS})
    void nonJsonGoesStraightToDlt(String source) {
        UUID orderId = UUID.randomUUID();
        double before = dltCounter(dltTopic(source));
        probe.send(source, orderId.toString(), "this-is-not-json", Map.of());
        assertPoisonGoesStraightToDlt(source, orderId, before);
    }

    @ParameterizedTest
    @ValueSource(strings = {Topics.ORDER_EVENTS, Topics.RESTAURANT_EVENTS})
    void unknownEventTypeGoesStraightToDlt(String source) throws Exception {
        UUID orderId = UUID.randomUUID();
        EventEnvelope valid = validEnvelope(source, orderId, UUID.randomUUID());
        ObjectNode node = (ObjectNode) JSON.readTree(EventCodec.toJson(valid));
        node.put("eventType", "NoSuchEvent");
        double before = dltCounter(dltTopic(source));
        probe.send(source, orderId.toString(), JSON.writeValueAsString(node), Map.of());
        assertPoisonGoesStraightToDlt(source, orderId, before);
    }

    @ParameterizedTest
    @ValueSource(strings = {Topics.ORDER_EVENTS, Topics.RESTAURANT_EVENTS})
    void correlationIdHeaderMismatchGoesStraightToDlt(String source) {
        UUID orderId = UUID.randomUUID();
        EventEnvelope valid = validEnvelope(source, orderId, UUID.randomUUID());
        double before = dltCounter(dltTopic(source));
        probe.send(source, orderId.toString(), EventCodec.toJson(valid), Map.of(
                EventHeaders.EVENT_TYPE, valid.eventType(),
                EventHeaders.EVENT_VERSION, "1",
                EventHeaders.CORRELATION_ID, UUID.randomUUID().toString()));
        assertPoisonGoesStraightToDlt(source, orderId, before);
    }

    // ------------------------------------------------------------------ exhausted

    @Test
    void exhaustedRetriesWalkAllRetryTopicsThenDltWithAlertAndCounter(CapturedOutput output) {
        UUID orderId = UUID.randomUUID();
        EventEnvelope accepted = Envelopes.first(EventTypes.ORDER_ACCEPTED, orderId, UUID.randomUUID(),
                new OrderAccepted(RESTAURANT, 15, PICKUP_ADDRESS));
        doThrow(new RuntimeException("always failing")).when(spy()).firstTime(accepted.eventId());
        String dlt = dltTopic(Topics.RESTAURANT_EVENTS);
        double before = dltCounter(dlt);
        publish(Topics.RESTAURANT_EVENTS, accepted);

        awaitRecord(dlt, orderId);
        for (int i = 0; i < 3; i++) {
            assertThat(read(retryTopic(Topics.RESTAURANT_EVENTS, i), orderId))
                    .as("retry-%d records", i).hasSize(1);
        }
        assertThat(read(dlt, orderId)).hasSize(1);

        await().atMost(WAIT).pollInterval(POLL).until(() -> output.getAll().lines()
                .anyMatch(l -> l.contains("ALERT") && l.contains(orderId.toString())));
        String line = output.getAll().lines()
                .filter(l -> l.contains("ALERT") && l.contains(orderId.toString())).findFirst().orElseThrow();
        assertThat(line).contains("ERROR");

        await().atMost(WAIT).pollInterval(POLL).until(() -> dltCounter(dlt) >= before + 1);
        assertThat(dltCounter(dlt)).isEqualTo(before + 1);
        assertThat(count("SELECT count(*) FROM deliveries WHERE order_id = ?", orderId)).isZero();
        assertThat(count("SELECT count(*) FROM processed_events WHERE event_id = ?", accepted.eventId())).isZero();
    }
}
