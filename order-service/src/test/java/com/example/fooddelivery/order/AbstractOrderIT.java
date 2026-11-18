package com.example.fooddelivery.order;

import static org.awaitility.Awaitility.await;

import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventHeaders;
import com.example.fooddelivery.events.Topics;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Support base class for order-service integration tests. Containers are static singletons shared by every
 * IT in the module (same image tags as docker-compose.yml). Every test uses fresh UUIDs.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class AbstractOrderIT {

    protected static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));
    protected static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16.10-alpine"))
                    .withDatabaseName("order_db")
                    .withUsername("app")
                    .withPassword("app");

    protected static final ObjectMapper JSON = new ObjectMapper();

    private static KafkaProducer<String, String> producer;

    static {
        KAFKA.start();
        POSTGRES.start();
        createTopics();
    }

    private static void createTopics() {
        Properties p = new Properties();
        p.put("bootstrap.servers", KAFKA.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(p)) {
            List<NewTopic> topics = new ArrayList<>();
            for (Topics.TopicSpec t : Topics.all()) {
                topics.add(new NewTopic(t.name(), t.partitions(), (short) 1));
            }
            admin.createTopics(topics).all().get(60, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot create topics", e);
        }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    protected TestRestTemplate rest;

    @BeforeAll
    static void startProducer() {
        if (producer == null) {
            Properties p = new Properties();
            p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
            p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            p.put(ProducerConfig.ACKS_CONFIG, "all");
            producer = new KafkaProducer<>(p);
        }
    }

    @AfterAll
    static void noop() {
        // containers and the producer live for the whole JVM (static singletons)
    }

    // ------------------------------------------------------------------ REST helpers

    protected static String orderJson(UUID customerId, UUID restaurantId, String itemsJson, String address) {
        StringBuilder sb = new StringBuilder("{");
        List<String> parts = new ArrayList<>();
        if (customerId != null) {
            parts.add("\"customerId\":\"" + customerId + "\"");
        }
        if (restaurantId != null) {
            parts.add("\"restaurantId\":\"" + restaurantId + "\"");
        }
        if (itemsJson != null) {
            parts.add("\"items\":" + itemsJson);
        }
        if (address != null) {
            parts.add("\"deliveryAddress\":\"" + address + "\"");
        }
        sb.append(String.join(",", parts)).append("}");
        return sb.toString();
    }

    protected static String itemJson(UUID menuItemId, int quantity, String unitPrice) {
        return "{\"menuItemId\":\"" + menuItemId + "\",\"quantity\":" + quantity + ",\"unitPrice\":" + unitPrice + "}";
    }

    protected static String validOrderJson() {
        return orderJson(UUID.randomUUID(), UUID.randomUUID(),
                "[" + itemJson(UUID.randomUUID(), 2, "12.50") + "]", "1 Main St");
    }

    protected ResponseEntity<JsonNode> postRaw(String path, String json) {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        return rest.exchange(path, org.springframework.http.HttpMethod.POST, new HttpEntity<>(json, h),
                JsonNode.class);
    }

    protected ResponseEntity<JsonNode> post(String path) {
        return rest.exchange(path, org.springframework.http.HttpMethod.POST, HttpEntity.EMPTY, JsonNode.class);
    }

    protected ResponseEntity<JsonNode> get(String path) {
        return rest.getForEntity(path, JsonNode.class);
    }

    /** POSTs a valid order and returns the 202 body. */
    protected JsonNode placeOrder() {
        ResponseEntity<JsonNode> r = postRaw("/orders", validOrderJson());
        if (r.getStatusCode().value() != 202) {
            throw new AssertionError("POST /orders expected 202 but was " + r.getStatusCode() + " " + r.getBody());
        }
        return r.getBody();
    }

    protected String status(UUID orderId) {
        ResponseEntity<JsonNode> r = get("/orders/" + orderId);
        if (r.getStatusCode().value() != 200 || r.getBody() == null) {
            return "HTTP_" + r.getStatusCode().value();
        }
        return r.getBody().get("status").asText();
    }

    protected void awaitStatus(UUID orderId, String expected) {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> org.assertj.core.api.Assertions.assertThat(status(orderId)).isEqualTo(expected));
    }

    /** History as "from>to" strings, with from rendered as "null" for the initial row. */
    protected List<String> history(UUID orderId) {
        ResponseEntity<JsonNode> r = get("/orders/" + orderId + "/history");
        List<String> out = new ArrayList<>();
        for (JsonNode n : r.getBody()) {
            String from = n.get("fromStatus").isNull() ? "null" : n.get("fromStatus").asText();
            out.add(from + ">" + n.get("toStatus").asText());
        }
        return out;
    }

    protected static List<String> historyOf(String... statuses) {
        List<String> out = new ArrayList<>();
        String prev = "null";
        for (String s : statuses) {
            out.add(prev + ">" + s);
            prev = s;
        }
        return out;
    }

    // ------------------------------------------------------------------ Kafka helpers

    /** A record read from order.events. */
    public record Received(String key, int partition, EventEnvelope envelope, Map<String, String> headers,
            String raw) {
        public String type() {
            return envelope.eventType();
        }
    }

    /** Reads order.events from the beginning (fresh consumer, no committed offsets) and returns the records for the key. */
    protected static List<Received> orderEventsFor(UUID orderId) {
        return readTopic(Topics.ORDER_EVENTS, orderId.toString());
    }

    protected static List<Received> orderEventsOfType(UUID orderId, String type) {
        return orderEventsFor(orderId).stream().filter(r -> r.type().equals(type)).collect(Collectors.toList());
    }

    protected static List<Received> readTopic(String topic, String key) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "it-reader-" + UUID.randomUUID());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        List<Received> out = new ArrayList<>();
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p)) {
            List<TopicPartition> tps = new ArrayList<>();
            for (PartitionInfo pi : c.partitionsFor(topic)) {
                tps.add(new TopicPartition(topic, pi.partition()));
            }
            c.assign(tps);
            c.seekToBeginning(tps);
            Map<TopicPartition, Long> end = c.endOffsets(tps);
            long deadline = System.currentTimeMillis() + 15_000;
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
                    if (key.equals(rec.key())) {
                        Map<String, String> headers = new HashMap<>();
                        for (Header h : rec.headers()) {
                            headers.put(h.key(), EventHeaders.text(h.value()));
                        }
                        out.add(new Received(rec.key(), rec.partition(), EventCodec.fromJson(rec.value()), headers,
                                rec.value()));
                    }
                }
            }
        }
        return out;
    }

    /** Waits (max 30 s) for at least one event of the type for the order on order.events and returns all matches. */
    protected static List<Received> awaitOrderEvents(UUID orderId, String type, int atLeast) {
        List<List<Received>> holder = new ArrayList<>();
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(300)).until(() -> {
            List<Received> found = orderEventsOfType(orderId, type);
            holder.clear();
            holder.add(found);
            return found.size() >= atLeast;
        });
        return holder.get(0);
    }

    protected static EventEnvelope awaitPlaced(UUID orderId) {
        return awaitOrderEvents(orderId, "OrderPlaced", 1).get(0).envelope();
    }

    /** Sends the envelope to the topic, key = envelope.orderId, with the standard headers. */
    protected static void send(String topic, EventEnvelope envelope) {
        sendRaw(topic, envelope.orderId().toString(), EventCodec.toJson(envelope), EventHeaders.from(envelope));
    }

    protected static void sendRaw(String topic, String key, String value, Map<String, byte[]> headers) {
        ProducerRecord<String, String> rec = new ProducerRecord<>(topic, key, value);
        for (Map.Entry<String, byte[]> h : headers.entrySet()) {
            rec.headers().add(new RecordHeader(h.getKey(), h.getValue()));
        }
        try {
            producer.send(rec).get(15, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("send failed", e);
        }
    }

    protected static Set<Integer> partitionsOf(List<Received> records) {
        return records.stream().map(Received::partition).collect(Collectors.toSet());
    }
}
