package com.example.fooddelivery.restaurant.support;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/** New test helper: produce and read raw (possibly non-JSON) string records, which the base class cannot do. */
public final class RestaurantKafkaRaw {

    private RestaurantKafkaRaw() {
    }

    public static void send(String bootstrap, String topic, String key, String value, Map<String, String> headers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            headers.forEach((k, v) -> record.headers().add(k, v.getBytes(StandardCharsets.UTF_8)));
            producer.send(record).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot send to " + topic, e);
        }
    }

    /** All records with the given key currently on the topic (reads from the beginning). */
    public static List<ConsumerRecord<String, String>> read(String bootstrap, String topic, String key) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "raw-reader-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        List<ConsumerRecord<String, String>> out = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(p -> new TopicPartition(topic, p.partition())).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (!caughtUp(consumer, partitions, end) && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(200))) {
                    if (key.equals(r.key())) {
                        out.add(r);
                    }
                }
            }
        }
        return out;
    }

    /** Total lag of the group on the topic: end offsets minus committed offsets (0 when fully consumed). */
    public static long groupLag(String bootstrap, String topic, String group) {
        Map<String, Object> adminProps = Map.of("bootstrap.servers", bootstrap);
        try (org.apache.kafka.clients.admin.AdminClient admin =
                     org.apache.kafka.clients.admin.AdminClient.create(adminProps)) {
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> committed =
                    admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                        .map(p -> new TopicPartition(topic, p.partition())).toList();
                long lag = 0;
                for (Map.Entry<TopicPartition, Long> end : consumer.endOffsets(partitions).entrySet()) {
                    org.apache.kafka.clients.consumer.OffsetAndMetadata c = committed.get(end.getKey());
                    lag += end.getValue() - (c == null ? 0 : c.offset());
                }
                return lag;
            }
        } catch (Exception e) {
            throw new IllegalStateException("Cannot compute lag of " + group + " on " + topic, e);
        }
    }

    private static boolean caughtUp(KafkaConsumer<String, String> consumer, List<TopicPartition> partitions,
                                    Map<TopicPartition, Long> end) {
        for (TopicPartition tp : partitions) {
            if (consumer.position(tp) < end.get(tp)) {
                return false;
            }
        }
        return true;
    }
}
