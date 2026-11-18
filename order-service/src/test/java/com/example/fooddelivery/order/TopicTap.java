package com.example.fooddelivery.order;

import com.example.fooddelivery.events.Topics;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Continuously reads (from the beginning, no committed offsets) the order-service retry topics and DLTs of both
 * source topics, so tests can detect a record on a retry hop within about 100 ms instead of re-reading the topic.
 */
final class TopicTap {

    record Seen(String topic, String key, String value) {
    }

    private static TopicTap instance;

    private final Map<String, List<Seen>> byTopic = new ConcurrentHashMap<>();

    static synchronized TopicTap instance() {
        if (instance == null) {
            instance = new TopicTap(AbstractOrderIT.KAFKA.getBootstrapServers());
        }
        return instance;
    }

    private TopicTap(String bootstrap) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "m67-tap-" + UUID.randomUUID());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        List<TopicPartition> tps = new ArrayList<>();
        for (String src : List.of(Topics.RESTAURANT_EVENTS, Topics.DELIVERY_EVENTS)) {
            List<String> topics = new ArrayList<>();
            for (int i = 0; i < Topics.RETRY_HOPS; i++) {
                topics.add(Topics.retry(src, Topics.ORDER_SERVICE, i));
            }
            topics.add(Topics.dlt(src, Topics.ORDER_SERVICE));
            for (String t : topics) {
                byTopic.put(t, new CopyOnWriteArrayList<>());
                for (PartitionInfo pi : consumer.partitionsFor(t)) {
                    tps.add(new TopicPartition(t, pi.partition()));
                }
            }
        }
        consumer.assign(tps);
        consumer.seekToBeginning(tps);
        Thread thread = new Thread(() -> {
            while (true) {
                try {
                    for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(100))) {
                        byTopic.get(r.topic()).add(new Seen(r.topic(), r.key(), r.value()));
                    }
                } catch (RuntimeException e) {
                    // broker paused or unreachable: keep polling
                }
            }
        }, "m67-topic-tap");
        thread.setDaemon(true);
        thread.start();
    }

    List<Seen> seen(String topic, Object key) {
        List<Seen> out = new ArrayList<>();
        for (Seen s : byTopic.get(topic)) {
            if (String.valueOf(key).equals(s.key())) {
                out.add(s);
            }
        }
        return out;
    }

    int count(String topic, Object key) {
        return seen(topic, key).size();
    }
}
