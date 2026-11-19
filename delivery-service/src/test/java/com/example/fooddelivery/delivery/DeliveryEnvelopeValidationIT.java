package com.example.fooddelivery.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.fooddelivery.delivery.support.DeliveryIntegrationTestBase;
import com.example.fooddelivery.delivery.support.KafkaProbe;
import com.example.fooddelivery.events.CancelReason;
import com.example.fooddelivery.events.Envelopes;
import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventHeaders;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.OrderCancelled;
import com.example.fooddelivery.events.Topics;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Value;

/**
 * Fix loop 1 (F8, F9): the decode step rejects wrong or missing version and headers straight to the DLT (no retry
 * hop), and the DLT publisher lets the key choose the partition, so a record from source partition 2 reaches the
 * 1-partition DLT.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DeliveryEnvelopeValidationIT extends DeliveryIntegrationTestBase {

    private static final String SOURCE = Topics.ORDER_EVENTS;
    private static final String DLT = SOURCE + "." + Topics.DELIVERY_SERVICE + ".dlt";

    @Value("${spring.kafka.bootstrap-servers}")
    String bootstrap;

    private KafkaProbe probe;

    @BeforeAll
    void init() {
        probe = new KafkaProbe(bootstrap);
    }

    private EventEnvelope valid(UUID orderId) {
        return Envelopes.first(EventTypes.ORDER_CANCELLED, orderId, UUID.randomUUID(),
                new OrderCancelled(CancelReason.CUSTOMER_REQUEST));
    }

    private void sendToPartition(int partition, UUID orderId, String value, Map<String, String> headers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(SOURCE, partition, orderId.toString(), value);
            headers.forEach((k, v) -> record.headers().add(k, v.getBytes(StandardCharsets.UTF_8)));
            producer.send(record).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot send", e);
        }
    }

    private static Map<String, String> headersOf(EventEnvelope e) {
        Map<String, String> h = new HashMap<>();
        EventHeaders.from(e).forEach((k, v) -> h.put(k, new String(v, StandardCharsets.UTF_8)));
        return h;
    }

    private void assertDltOnlyNoRetryHops(UUID orderId) {
        await().atMost(WAIT).pollInterval(POLL).until(() -> !probe.read(DLT, orderId.toString()).isEmpty());
        assertThat(probe.read(DLT, orderId.toString())).hasSize(1);
        await().during(QUIET).atMost(WAIT).pollInterval(POLL).until(() -> {
            for (int i = 0; i < 3; i++) {
                if (!probe.read(SOURCE + "." + Topics.DELIVERY_SERVICE + ".retry-" + i, orderId.toString())
                        .isEmpty()) {
                    return false;
                }
            }
            return true;
        });
        assertThat(jdbc.queryForObject("SELECT count(*) FROM deliveries WHERE order_id = ?", Long.class, orderId))
                .isZero();
    }

    @Test
    void eventVersion2EnvelopeGoesStraightToDlt() throws Exception {
        UUID orderId = UUID.randomUUID();
        EventEnvelope env = valid(orderId);
        ObjectNode node = (ObjectNode) JSON.readTree(EventCodec.toJson(env));
        node.put("eventVersion", 2);
        Map<String, String> headers = headersOf(env);
        headers.put(EventHeaders.EVENT_VERSION, "2");
        probe.send(SOURCE, orderId.toString(), JSON.writeValueAsString(node), headers);
        assertDltOnlyNoRetryHops(orderId);
    }

    @Test
    void missingEventVersionHeaderGoesStraightToDlt() {
        UUID orderId = UUID.randomUUID();
        EventEnvelope env = valid(orderId);
        Map<String, String> headers = headersOf(env);
        headers.remove(EventHeaders.EVENT_VERSION);
        probe.send(SOURCE, orderId.toString(), EventCodec.toJson(env), headers);
        assertDltOnlyNoRetryHops(orderId);
    }

    @Test
    void poisonFromSourcePartition2ReachesTheSinglePartitionDlt() {
        UUID orderId = UUID.randomUUID();
        sendToPartition(2, orderId, "this-is-not-json", Map.of());
        await().atMost(WAIT).pollInterval(POLL).until(() -> !probe.read(DLT, orderId.toString()).isEmpty());
        List<KafkaProbe.Rec> recs = probe.read(DLT, orderId.toString());
        assertThat(recs).hasSize(1);
        assertThat(recs.get(0).partition()).isZero();
    }
}
