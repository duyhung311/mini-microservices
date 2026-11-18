package com.example.fooddelivery.delivery.support;

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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/** Raw (non-decoding) Kafka access for tests: send arbitrary values and headers, read any topic by key. */
public final class KafkaProbe {

    /** One record as read from a topic. */
    public record Rec(String topic, int partition, String key, String value, Map<String, String> headers) {
    }

    private final String bootstrap;

    public KafkaProbe(String bootstrap) {
        this.bootstrap = bootstrap;
    }

    public void send(String topic, String key, String value, Map<String, String> headers) {
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

    /** Reads the whole topic from the beginning and returns the records with the given key. */
    public List<Rec> read(String topic, String key) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-probe-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        List<Rec> out = new ArrayList<>();
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
                        Map<String, String> headers = new HashMap<>();
                        for (Header h : r.headers()) {
                            headers.put(h.key(), new String(h.value(), StandardCharsets.UTF_8));
                        }
                        out.add(new Rec(topic, r.partition(), r.key(), r.value(), headers));
                    }
                }
            }
        }
        return out;
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
