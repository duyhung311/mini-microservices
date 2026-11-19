package com.example.fooddelivery.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.events.Topics;
import java.time.Duration;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/** F8: a poison record from source partition 2 must land on the 1-partition DLT (partition resolver returns null). */
class OrderDltPartitionIT extends AbstractOrderIT {

    private final TopicTap tap = TopicTap.instance();

    @Test
    void poisonFromSourcePartition2LandsOnTheSinglePartitionDlt() throws Exception {
        UUID key = UUID.randomUUID();
        String value = "this-is-not-json";
        org.apache.kafka.clients.producer.RecordMetadata md = sendToPartition(Topics.RESTAURANT_EVENTS, 2, key, value);
        assertThat(md.partition()).isEqualTo(2);

        String dlt = Topics.dlt(Topics.RESTAURANT_EVENTS, Topics.ORDER_SERVICE);
        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100))
                .until(() -> tap.count(dlt, key) >= 1);
        assertThat(tap.count(dlt, key)).isEqualTo(1);
        assertThat(tap.seen(dlt, key).get(0).value()).isEqualTo(value);
    }

    private static org.apache.kafka.clients.producer.RecordMetadata sendToPartition(String topic, int partition,
            UUID key, String value) throws Exception {
        java.util.Properties p = new java.util.Properties();
        p.put("bootstrap.servers", KAFKA.getBootstrapServers());
        p.put("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class.getName());
        p.put("value.serializer", org.apache.kafka.common.serialization.StringSerializer.class.getName());
        try (org.apache.kafka.clients.producer.KafkaProducer<String, String> producer =
                new org.apache.kafka.clients.producer.KafkaProducer<>(p)) {
            return producer.send(new ProducerRecord<>(topic, partition, key.toString(), value)).get();
        }
    }
}
