package com.example.fooddelivery.order;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/** Reads whole topics once (fresh consumer, from the beginning) and keeps the records whose key matches. */
final class TopicScan {

    private TopicScan() {
    }

    static List<ConsumerRecord<String, String>> read(List<String> topics, Predicate<String> keyFilter) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AbstractOrderIT.KAFKA.getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "it-scan-" + UUID.randomUUID());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        List<ConsumerRecord<String, String>> out = new ArrayList<>();
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p)) {
            List<TopicPartition> tps = new ArrayList<>();
            for (String topic : topics) {
                for (PartitionInfo pi : c.partitionsFor(topic)) {
                    tps.add(new TopicPartition(topic, pi.partition()));
                }
            }
            c.assign(tps);
            c.seekToBeginning(tps);
            Map<TopicPartition, Long> end = c.endOffsets(tps);
            long deadline = System.currentTimeMillis() + 20_000;
            while (System.currentTimeMillis() < deadline) {
                boolean done = true;
                for (TopicPartition tp : tps) {
                    if (c.position(tp) < end.get(tp)) {
                        done = false;
                    }
                }
                if (done) {
                    break;
                }
                for (ConsumerRecord<String, String> rec : c.poll(Duration.ofMillis(200))) {
                    if (rec.key() != null && keyFilter.test(rec.key())) {
                        out.add(rec);
                    }
                }
            }
        }
        return out;
    }
}
