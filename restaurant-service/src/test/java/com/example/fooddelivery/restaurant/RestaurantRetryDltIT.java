package com.example.fooddelivery.restaurant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.fooddelivery.events.DeliveryFailed;
import com.example.fooddelivery.events.DeliveryFailureReason;
import com.example.fooddelivery.events.Envelopes;
import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventHeaders;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.Topics;
import com.example.fooddelivery.restaurant.support.LiveContexts;
import com.example.fooddelivery.restaurant.support.RestaurantFaults;
import com.example.fooddelivery.restaurant.support.RestaurantIntegrationTestBase;
import com.example.fooddelivery.restaurant.support.RestaurantKafkaRaw;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.test.context.TestPropertySource;

/**
 * Criterion 18: transient failures retry and succeed, poison goes straight to the DLT, exhaustion alerts.
 * Failures are injected with a database trigger (see RestaurantFaults) so they hit whichever listener handles the
 * record; the listeners of other cached contexts are silenced for the duration of this class.
 */
@ExtendWith(OutputCaptureExtension.class)
@TestPropertySource(properties = {
        "app.kafka.retry.delay-ms=200", "app.kafka.retry.multiplier=2", "app.kafka.retry.max-delay-ms=1000"})
class RestaurantRetryDltIT extends RestaurantIntegrationTestBase {

    private static List<KafkaListenerEndpointRegistry> silenced;

    @Autowired
    private ApplicationContext context;

    @Autowired
    private MeterRegistry meters;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrap;

    @BeforeEach
    void silenceOtherContexts() {
        if (silenced == null) {
            silenced = LiveContexts.stopListenersExcept(context);
        }
    }

    @AfterAll
    static void restoreOtherContexts() {
        if (silenced != null) {
            LiveContexts.restart(silenced);
            silenced = null;
        }
    }

    private static String retryTopic(String source, int hop) {
        return Topics.retry(source, Topics.RESTAURANT_SERVICE, hop);
    }

    private static String dltTopic(String source) {
        return Topics.dlt(source, Topics.RESTAURANT_SERVICE);
    }

    private int rawCount(String topic, String key) {
        return RestaurantKafkaRaw.read(bootstrap, topic, key).size();
    }

    private double dltCounter(String topic) {
        Counter counter = meters.find("kafka.dlt.records").tag("topic", topic).counter();
        return counter == null ? 0.0 : counter.count();
    }

    private static String alert(String topic, String key) {
        return "ALERT dlt topic=" + topic + " key=" + key;
    }

    private void awaitOutputContains(CapturedOutput output, String text) {
        await().atMost(WAIT).pollInterval(POLL).until(() -> output.getAll().contains(text));
    }

    private EventEnvelope validEnvelopeFor(String source) {
        EventEnvelope placed = RestaurantFaults.orderPlaced(PASTA_PLACE, MARGHERITA);
        if (Topics.ORDER_EVENTS.equals(source)) {
            return placed;
        }
        return Envelopes.next(placed, EventTypes.DELIVERY_FAILED,
                new DeliveryFailed(DeliveryFailureReason.NO_DRIVER_AVAILABLE));
    }

    private void assertPoisonDeadLettered(String source, String key, CapturedOutput output) {
        String dlt = dltTopic(source);
        await().atMost(WAIT).pollInterval(POLL).until(() -> rawCount(dlt, key) >= 1);
        awaitOutputContains(output, alert(dlt, key));
        await().during(QUIET).atMost(WAIT).pollInterval(POLL).untilAsserted(() -> {
            for (int hop = 0; hop < Topics.RETRY_HOPS; hop++) {
                assertThat(rawCount(retryTopic(source, hop), key))
                        .as("records on %s", retryTopic(source, hop)).isZero();
            }
            assertThat(rawCount(dlt, key)).isEqualTo(1);
        });
    }

    // ------------------------------------------------------------------ transient

    @Test
    void transientFailureOnOrderEventsGoesThroughRetry0AndSucceeds() {
        EventEnvelope placed = RestaurantFaults.orderPlaced(PASTA_PLACE, MARGHERITA);
        try (RestaurantFaults.Fault fault = RestaurantFaults.failProcessedEventsInsert(jdbc, placed.eventId(), 1)) {
            publish(Topics.ORDER_EVENTS, placed);

            awaitTicketStatus(placed.orderId(), "ACCEPTED");
            assertThat(fault.attempts()).isEqualTo(2);
        }
        assertThat(eventsOfType(retryTopic(Topics.ORDER_EVENTS, 0), placed.orderId(), EventTypes.ORDER_PLACED))
                .hasSize(1);
        assertThat(eventsFor(retryTopic(Topics.ORDER_EVENTS, 1), placed.orderId())).isEmpty();
        assertThat(eventsFor(dltTopic(Topics.ORDER_EVENTS), placed.orderId())).isEmpty();
        assertThat(awaitEvents(Topics.RESTAURANT_EVENTS, placed.orderId(), EventTypes.ORDER_ACCEPTED)).hasSize(1);
    }

    @Test
    void transientFailureOnDeliveryEventsGoesThroughRetry0AndSucceeds() {
        EventEnvelope placed = placeOrder(PASTA_PLACE, MARGHERITA);
        awaitTicketStatus(placed.orderId(), "ACCEPTED");
        EventEnvelope failed = Envelopes.next(placed, EventTypes.DELIVERY_FAILED,
                new DeliveryFailed(DeliveryFailureReason.NO_DRIVER_AVAILABLE));
        try (RestaurantFaults.Fault fault = RestaurantFaults.failProcessedEventsInsert(jdbc, failed.eventId(), 1)) {
            publish(Topics.DELIVERY_EVENTS, failed);

            awaitTicketStatus(placed.orderId(), "ABANDONED");
            assertThat(fault.attempts()).isEqualTo(2);
        }
        assertThat(eventsOfType(retryTopic(Topics.DELIVERY_EVENTS, 0), placed.orderId(),
                EventTypes.DELIVERY_FAILED)).hasSize(1);
        assertThat(eventsFor(dltTopic(Topics.DELIVERY_EVENTS), placed.orderId())).isEmpty();
    }

    // ------------------------------------------------------------------ poison

    @ParameterizedTest
    @ValueSource(strings = {Topics.ORDER_EVENTS, Topics.DELIVERY_EVENTS})
    void nonJsonGoesStraightToDltWithoutRetryHops(String source, CapturedOutput output) {
        String key = UUID.randomUUID().toString();
        RestaurantKafkaRaw.send(bootstrap, source, key, "this-is-not-json", Map.of());

        assertPoisonDeadLettered(source, key, output);
    }

    @ParameterizedTest
    @ValueSource(strings = {Topics.ORDER_EVENTS, Topics.DELIVERY_EVENTS})
    void unknownEventTypeGoesStraightToDltWithoutRetryHops(String source, CapturedOutput output) throws Exception {
        EventEnvelope valid = validEnvelopeFor(source);
        ObjectNode node = (ObjectNode) JSON.readTree(EventCodec.toJson(valid));
        node.put("eventType", "SomethingUnheardOf");
        String key = valid.orderId().toString();
        RestaurantKafkaRaw.send(bootstrap, source, key, JSON.writeValueAsString(node), Map.of(
                EventHeaders.EVENT_TYPE, "SomethingUnheardOf",
                EventHeaders.EVENT_VERSION, "1",
                EventHeaders.CORRELATION_ID, valid.correlationId().toString()));

        assertPoisonDeadLettered(source, key, output);
    }

    @ParameterizedTest
    @ValueSource(strings = {Topics.ORDER_EVENTS, Topics.DELIVERY_EVENTS})
    void correlationIdHeaderMismatchGoesStraightToDltWithoutRetryHops(String source, CapturedOutput output) {
        EventEnvelope valid = validEnvelopeFor(source);
        String key = valid.orderId().toString();
        RestaurantKafkaRaw.send(bootstrap, source, key, EventCodec.toJson(valid), Map.of(
                EventHeaders.EVENT_TYPE, valid.eventType(),
                EventHeaders.EVENT_VERSION, "1",
                EventHeaders.CORRELATION_ID, UUID.randomUUID().toString()));

        assertPoisonDeadLettered(source, key, output);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM processed_events WHERE event_id = ?", Long.class,
                valid.eventId())).isZero();
    }

    // ------------------------------------------------------------------ exhausted

    @Test
    void exhaustedRetriesOnOrderEventsEndOnDltWithAlertAndCounter(CapturedOutput output) {
        String dlt = dltTopic(Topics.ORDER_EVENTS);
        double before = dltCounter(dlt);
        EventEnvelope placed = RestaurantFaults.orderPlaced(PASTA_PLACE, MARGHERITA);
        try (RestaurantFaults.Fault fault =
                     RestaurantFaults.failProcessedEventsInsert(jdbc, placed.eventId(), Long.MAX_VALUE / 2)) {
            publish(Topics.ORDER_EVENTS, placed);

            for (int hop = 0; hop < Topics.RETRY_HOPS; hop++) {
                assertThat(awaitEvents(retryTopic(Topics.ORDER_EVENTS, hop), placed.orderId(),
                        EventTypes.ORDER_PLACED)).as("retry-%d", hop).hasSize(1);
            }
            assertThat(awaitEvents(dlt, placed.orderId(), EventTypes.ORDER_PLACED)).hasSize(1);
            assertThat(fault.attempts()).isEqualTo(4);
        }
        awaitOutputContains(output, alert(dlt, placed.orderId().toString()) + " eventId=" + placed.eventId());
        await().atMost(WAIT).pollInterval(POLL).until(() -> dltCounter(dlt) >= before + 1);
        assertThat(output.getAll()).contains("ERROR").contains("ALERT");
        assertThat(ticketStatus(placed.orderId())).isNull();
    }

    @Test
    void exhaustedRetriesOnDeliveryEventsEndOnDltWithAlertAndCounter(CapturedOutput output) {
        String dlt = dltTopic(Topics.DELIVERY_EVENTS);
        EventEnvelope placed = placeOrder(PASTA_PLACE, MARGHERITA);
        awaitTicketStatus(placed.orderId(), "ACCEPTED");
        EventEnvelope failed = Envelopes.next(placed, EventTypes.DELIVERY_FAILED,
                new DeliveryFailed(DeliveryFailureReason.NO_DRIVER_AVAILABLE));
        double before = dltCounter(dlt);
        try (RestaurantFaults.Fault fault =
                     RestaurantFaults.failProcessedEventsInsert(jdbc, failed.eventId(), Long.MAX_VALUE / 2)) {
            publish(Topics.DELIVERY_EVENTS, failed);

            for (int hop = 0; hop < Topics.RETRY_HOPS; hop++) {
                assertThat(awaitEvents(retryTopic(Topics.DELIVERY_EVENTS, hop), placed.orderId(),
                        EventTypes.DELIVERY_FAILED)).as("retry-%d", hop).hasSize(1);
            }
            assertThat(awaitEvents(dlt, placed.orderId(), EventTypes.DELIVERY_FAILED)).hasSize(1);
            assertThat(fault.attempts()).isEqualTo(4);
        }
        awaitOutputContains(output, alert(dlt, placed.orderId().toString()) + " eventId=" + failed.eventId());
        await().atMost(WAIT).pollInterval(POLL).until(() -> dltCounter(dlt) >= before + 1);
        assertThat(ticketStatus(placed.orderId())).isEqualTo("ACCEPTED");
    }
}
