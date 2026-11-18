package com.example.fooddelivery.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventHeaders;
import com.example.fooddelivery.events.Topics;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

/**
 * Criterion 24 (spec 14, ordering test): three order-service instances (this base context plus two more, all with
 * server.port=0, the same Postgres database and the same consumer group) process 20 orders x 5 saga events that are
 * published at once and without waiting. The restaurant and delivery events travel on different topics, so their
 * arrival order is not guaranteed; PREMATURE ones must converge through the retry topics.
 * The timeout job is disabled in all three instances so that it cannot interfere. The test context is dirtied after
 * the class so that it does not stay in the consumer group and steal partitions from later ITs (e.g. OrderRetryDltIT
 * reads counters of the shared context).
 *
 * Other cached contexts of this JVM may be members of the same group too; the assertions are about outcomes only,
 * so it does not matter which member processes a record.
 */
@TestPropertySource(properties = "orders.timeout.enabled=false")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class OrderingIT extends AbstractOrderIT {

    private static final int ORDERS = 20;
    private static final List<ConfigurableApplicationContext> EXTRA = new ArrayList<>();

    @AfterAll
    static void closeExtra() {
        for (ConfigurableApplicationContext c : EXTRA) {
            c.close();
        }
        EXTRA.clear();
    }

    private static ConfigurableApplicationContext startInstance() {
        return new SpringApplicationBuilder(OrderApplication.class)
                .run("--server.port=0",
                        "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                        "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                        "--spring.datasource.username=" + POSTGRES.getUsername(),
                        "--spring.datasource.password=" + POSTGRES.getPassword(),
                        "--app.kafka.retry.delay-ms=200",
                        "--app.kafka.retry.multiplier=2",
                        "--app.kafka.retry.max-delay-ms=1000",
                        "--orders.timeout.enabled=false");
    }

    @Test
    void threeInstancesDeliverTwentyOrdersInAValidOrderWithoutDeadLetters() throws Exception {
        EXTRA.add(startInstance());
        EXTRA.add(startInstance());

        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < ORDERS; i++) {
            ids.add(UUID.fromString(placeOrder().get("orderId").asText()));
        }
        List<EventEnvelope> placedEvents = new ArrayList<>();
        for (UUID id : ids) {
            placedEvents.add(awaitPlaced(id));
        }

        // build the 5 events of every order, then publish all 100 without waiting for any acknowledgement
        List<ProducerRecord<String, String>> records = new ArrayList<>();
        for (EventEnvelope placed : placedEvents) {
            UUID driver = UUID.randomUUID();
            EventEnvelope accepted = SagaEvents.accepted(placed);
            EventEnvelope assigned = SagaEvents.driverAssigned(accepted, driver);
            EventEnvelope ready = SagaEvents.ready(accepted);
            EventEnvelope pickedUp = SagaEvents.pickedUp(assigned, driver);
            EventEnvelope delivered = SagaEvents.delivered(pickedUp, driver);
            records.add(record(Topics.RESTAURANT_EVENTS, accepted));
            records.add(record(Topics.DELIVERY_EVENTS, assigned));
            records.add(record(Topics.RESTAURANT_EVENTS, ready));
            records.add(record(Topics.DELIVERY_EVENTS, pickedUp));
            records.add(record(Topics.DELIVERY_EVENTS, delivered));
        }
        // per order the events are sent in causal order; cross-topic arrival order is still not guaranteed
        List<ProducerRecord<String, String>> shuffled = new ArrayList<>();
        for (int step = 0; step < 5; step++) {
            for (int o = 0; o < ORDERS; o++) {
                shuffled.add(records.get(o * 5 + step));
            }
        }

        java.util.Properties p = new java.util.Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        List<Future<RecordMetadata>> acks = new ArrayList<>();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
            for (ProducerRecord<String, String> r : shuffled) {
                acks.add(producer.send(r));
            }
            producer.flush();
            for (Future<RecordMetadata> f : acks) {
                f.get();
            }
        }

        List<String> happyA = historyOf("PLACED", "ACCEPTED", "DRIVER_ASSIGNED", "READY_FOR_PICKUP",
                "OUT_FOR_DELIVERY", "DELIVERED");
        List<String> happyB = historyOf("PLACED", "ACCEPTED", "READY_FOR_PICKUP", "DRIVER_ASSIGNED",
                "OUT_FOR_DELIVERY", "DELIVERED");

        Awaitility.await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(500)).untilAsserted(() -> {
            for (UUID id : ids) {
                assertThat(status(id)).as("status of %s", id).isEqualTo("DELIVERED");
            }
        });
        for (UUID id : ids) {
            assertThat(history(id)).as("history of %s", id).isIn(happyA, happyB);
        }

        // nothing was dead-lettered (all 6 DLTs), and the histories stay as they are
        Set<String> keys = ids.stream().map(UUID::toString).collect(Collectors.toSet());
        List<String> dlts = new ArrayList<>();
        for (Topics.Subscription s : Topics.subscriptions()) {
            dlts.add(Topics.dlt(s.topic(), s.group()));
        }
        assertThat(dlts).hasSize(6);
        Awaitility.await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            List<ConsumerRecord<String, String>> dead = TopicScan.read(dlts, keys::contains);
            assertThat(dead).as("records on the DLTs for these orders").isEmpty();
            for (UUID id : ids) {
                assertThat(status(id)).isEqualTo("DELIVERED");
            }
        });
    }

    private static ProducerRecord<String, String> record(String topic, EventEnvelope e) {
        ProducerRecord<String, String> rec = new ProducerRecord<>(topic, e.orderId().toString(), EventCodec.toJson(e));
        for (java.util.Map.Entry<String, byte[]> h : EventHeaders.from(e).entrySet()) {
            rec.headers().add(new RecordHeader(h.getKey(), h.getValue()));
        }
        return rec;
    }
}
