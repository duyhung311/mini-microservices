package com.example.fooddelivery.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.events.Envelopes;
import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventHeaders;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.OrderAccepted;
import com.example.fooddelivery.events.Topics;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/** F9: an envelope eventVersion other than 1, or a missing eventVersion header, is a poison record (DLT, no retry). */
class OrderEventVersionIT extends AbstractOrderIT {

    private final TopicTap tap = TopicTap.instance();

    private static EventEnvelope accepted(UUID key) {
        return Envelopes.first(EventTypes.ORDER_ACCEPTED, key, UUID.randomUUID(),
                new OrderAccepted(UUID.randomUUID(), 15, "1 Kitchen St"));
    }

    private void assertStraightToDlt(String source, UUID key, String value) {
        String dlt = Topics.dlt(source, Topics.ORDER_SERVICE);
        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100))
                .until(() -> tap.count(dlt, key) >= 1);
        assertThat(tap.count(dlt, key)).isEqualTo(1);
        assertThat(tap.seen(dlt, key).get(0).value()).isEqualTo(value);
        Awaitility.await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            for (int i = 0; i < Topics.RETRY_HOPS; i++) {
                assertThat(tap.count(Topics.retry(source, Topics.ORDER_SERVICE, i), key))
                        .as("retry-%d records for %s", i, key).isZero();
            }
        });
    }

    @Test
    void envelopeVersion2GoesStraightToDltWithZeroRetryHops() throws Exception {
        UUID key = UUID.randomUUID();
        EventEnvelope env = accepted(key);
        ObjectNode node = (ObjectNode) JSON.readTree(EventCodec.toJson(env));
        node.put("eventVersion", 2);
        String value = JSON.writeValueAsString(node);
        // headers agree with the envelope (version 2), so only the version rule can reject it
        Map<String, byte[]> headers = EventHeaders.from(env);
        headers.put(EventHeaders.EVENT_VERSION, EventHeaders.utf8("2"));

        sendRaw(Topics.RESTAURANT_EVENTS, key.toString(), value, headers);

        assertStraightToDlt(Topics.RESTAURANT_EVENTS, key, value);
    }

    @Test
    void missingEventVersionHeaderGoesStraightToDltWithZeroRetryHops() {
        UUID key = UUID.randomUUID();
        EventEnvelope env = accepted(key);
        Map<String, byte[]> headers = EventHeaders.from(env);
        headers.remove(EventHeaders.EVENT_VERSION);

        sendRaw(Topics.RESTAURANT_EVENTS, key.toString(), EventCodec.toJson(env), headers);

        assertStraightToDlt(Topics.RESTAURANT_EVENTS, key, EventCodec.toJson(env));
    }
}
