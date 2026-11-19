package com.example.fooddelivery.restaurant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventHeaders;
import com.example.fooddelivery.events.Topics;
import com.example.fooddelivery.restaurant.support.RestaurantFaults;
import com.example.fooddelivery.restaurant.support.RestaurantIntegrationTestBase;
import com.example.fooddelivery.restaurant.support.RestaurantKafkaRaw;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;

/** F8 and F9: version and header validation sends poison straight to the DLT; DLT publishing ignores source partition. */
class RestaurantEventValidationIT extends RestaurantIntegrationTestBase {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrap;

    private int rawCount(String topic, String key) {
        return RestaurantKafkaRaw.read(bootstrap, topic, key).size();
    }

    private void assertStraightToDlt(String key) {
        String dlt = Topics.dlt(Topics.ORDER_EVENTS, Topics.RESTAURANT_SERVICE);
        await().atMost(WAIT).pollInterval(POLL).until(() -> rawCount(dlt, key) >= 1);
        await().during(QUIET).atMost(WAIT).pollInterval(POLL).untilAsserted(() -> {
            for (int hop = 0; hop < Topics.RETRY_HOPS; hop++) {
                assertThat(rawCount(Topics.retry(Topics.ORDER_EVENTS, Topics.RESTAURANT_SERVICE, hop), key))
                        .as("retry hop %d", hop).isZero();
            }
            assertThat(rawCount(dlt, key)).isEqualTo(1);
        });
    }

    @Test
    void eventVersion2GoesStraightToDlt() throws Exception {
        EventEnvelope valid = RestaurantFaults.orderPlaced(PASTA_PLACE, MARGHERITA);
        ObjectNode node = (ObjectNode) JSON.readTree(EventCodec.toJson(valid));
        node.put("eventVersion", 2);
        String key = valid.orderId().toString();
        RestaurantKafkaRaw.send(bootstrap, Topics.ORDER_EVENTS, key, JSON.writeValueAsString(node), Map.of(
                EventHeaders.EVENT_TYPE, valid.eventType(),
                EventHeaders.EVENT_VERSION, "2",
                EventHeaders.CORRELATION_ID, valid.correlationId().toString()));

        assertStraightToDlt(key);
        assertThat(ticketStatus(valid.orderId())).isNull();
    }

    @Test
    void missingEventVersionHeaderGoesStraightToDlt() {
        EventEnvelope valid = RestaurantFaults.orderPlaced(PASTA_PLACE, MARGHERITA);
        String key = valid.orderId().toString();
        RestaurantKafkaRaw.send(bootstrap, Topics.ORDER_EVENTS, key, EventCodec.toJson(valid), Map.of(
                EventHeaders.EVENT_TYPE, valid.eventType(),
                EventHeaders.CORRELATION_ID, valid.correlationId().toString()));

        assertStraightToDlt(key);
        assertThat(ticketStatus(valid.orderId())).isNull();
    }

    @Test
    void poisonFromSourcePartition2LandsOnTheSinglePartitionDlt() throws Exception {
        String key = UUID.randomUUID().toString();
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", bootstrap);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(Topics.ORDER_EVENTS, 2, key, "this-is-not-json");
            record.headers().add("x", "y".getBytes(StandardCharsets.UTF_8));
            assertThat(producer.send(record).get(10, TimeUnit.SECONDS).partition()).isEqualTo(2);
        }

        String dlt = Topics.dlt(Topics.ORDER_EVENTS, Topics.RESTAURANT_SERVICE);
        await().atMost(WAIT).pollInterval(POLL).until(() -> rawCount(dlt, key) >= 1);
        assertThat(RestaurantKafkaRaw.read(bootstrap, dlt, key)).hasSize(1)
                .allSatisfy(r -> assertThat(r.partition()).isZero());
    }
}
