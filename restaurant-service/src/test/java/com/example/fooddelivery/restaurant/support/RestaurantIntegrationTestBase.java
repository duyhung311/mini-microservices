package com.example.fooddelivery.restaurant.support;

import static org.awaitility.Awaitility.await;

import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventHeaders;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.Envelopes;
import com.example.fooddelivery.events.OrderPlaced;
import com.example.fooddelivery.events.Topics;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Support base class: static singleton Kafka and Postgres containers (same image tags as Compose),
 * all 27 topics created once from {@link Topics#all()}, RANDOM_PORT web server, plain Kafka
 * producer/consumer helpers and a small HTTP client. Seed data is shared, so the seed state is
 * restored (and tickets removed) before and after every test.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class RestaurantIntegrationTestBase {

    protected static final UUID PASTA_PLACE = UUID.fromString("11111111-1111-1111-1111-111111111111");
    protected static final UUID NIGHT_GRILL = UUID.fromString("11111111-1111-1111-1111-111111111112");
    protected static final UUID MARGHERITA = UUID.fromString("22222222-2222-2222-2222-000000000001");
    protected static final UUID LASAGNE = UUID.fromString("22222222-2222-2222-2222-000000000002");
    protected static final UUID TRUFFLE = UUID.fromString("22222222-2222-2222-2222-000000000003");

    protected static final Duration WAIT = Duration.ofSeconds(30);
    protected static final Duration POLL = Duration.ofMillis(200);
    protected static final Duration QUIET = Duration.ofSeconds(3);

    protected static final ObjectMapper JSON = new ObjectMapper();

    private static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));
    private static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16.10-alpine"));
    private static final KafkaProducer<String, String> PRODUCER;
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    static {
        KAFKA.start();
        POSTGRES.start();
        createTopics();
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        PRODUCER = new KafkaProducer<>(props);
    }

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @LocalServerPort
    protected int port;

    @Autowired
    protected JdbcTemplate jdbc;

    private static void createTopics() {
        Map<String, Object> props = Map.of("bootstrap.servers", KAFKA.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(props)) {
            for (Topics.TopicSpec spec : Topics.all()) {
                try {
                    admin.createTopics(List.of(new NewTopic(spec.name(), spec.partitions(), (short) 1)))
                            .all().get(30, TimeUnit.SECONDS);
                } catch (ExecutionException e) {
                    if (!(e.getCause() instanceof TopicExistsException)) {
                        throw new IllegalStateException("Cannot create topic " + spec.name(), e);
                    }
                } catch (Exception e) {
                    throw new IllegalStateException("Cannot create topic " + spec.name(), e);
                }
            }
        }
    }

    // ------------------------------------------------------------------ state reset

    @BeforeEach
    protected void resetBefore() {
        resetSharedState();
    }

    @AfterEach
    protected void resetAfter() {
        resetSharedState();
    }

    private void resetSharedState() {
        jdbc.update("DELETE FROM kitchen_tickets");
        jdbc.update("UPDATE restaurants SET is_open = true, max_concurrent_orders = 5 WHERE id = ?", PASTA_PLACE);
        jdbc.update("UPDATE restaurants SET is_open = false WHERE id = ?", NIGHT_GRILL);
    }

    protected long acceptedTickets(UUID restaurantId) {
        Long n = jdbc.queryForObject(
                "SELECT count(*) FROM kitchen_tickets WHERE restaurant_id = ? AND status = 'ACCEPTED'",
                Long.class, restaurantId);
        return n == null ? 0 : n;
    }

    // ------------------------------------------------------------------ Kafka helpers

    /** Publishes an envelope with key = orderId and the standard headers. */
    protected void publish(String topic, EventEnvelope envelope) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(topic, envelope.orderId().toString(), EventCodec.toJson(envelope));
        EventHeaders.from(envelope).forEach((name, value) -> record.headers().add(name, value));
        try {
            PRODUCER.send(record).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot publish to " + topic, e);
        }
    }

    /** Builds an OrderPlaced envelope (REST-originated style: causationId null) and publishes it to order.events. */
    protected EventEnvelope placeOrder(UUID restaurantId, UUID... itemIds) {
        UUID orderId = UUID.randomUUID();
        List<OrderPlaced.Item> items = new ArrayList<>();
        for (UUID itemId : itemIds) {
            items.add(new OrderPlaced.Item(itemId, 1, new BigDecimal("12.50")));
        }
        OrderPlaced payload = new OrderPlaced(UUID.randomUUID(), restaurantId, items,
                new BigDecimal("12.50").multiply(BigDecimal.valueOf(itemIds.length)), "5 Customer Ave");
        EventEnvelope envelope = Envelopes.first(EventTypes.ORDER_PLACED, orderId, UUID.randomUUID(), payload);
        publish(Topics.ORDER_EVENTS, envelope);
        return envelope;
    }

    /** One record read from a topic. */
    public record Published(String key, int partition, Map<String, String> headers, EventEnvelope envelope) {
        public String type() {
            return envelope.eventType();
        }
    }

    /** Reads the whole topic from the beginning with a fresh consumer and keeps the records whose key is orderId. */
    protected List<Published> eventsFor(String topic, UUID orderId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-reader-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        List<Published> out = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(p -> new TopicPartition(topic, p.partition())).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (!caughtUp(consumer, partitions, end) && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(200))) {
                    if (orderId.toString().equals(r.key())) {
                        Map<String, String> headers = new HashMap<>();
                        for (Header h : r.headers()) {
                            headers.put(h.key(), new String(h.value(), java.nio.charset.StandardCharsets.UTF_8));
                        }
                        out.add(new Published(r.key(), r.partition(), headers, EventCodec.fromJson(r.value())));
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

    protected List<Published> eventsOfType(String topic, UUID orderId, String eventType) {
        return eventsFor(topic, orderId).stream().filter(p -> p.type().equals(eventType))
                .collect(Collectors.toList());
    }

    /** Waits (max 30 s) until the topic holds at least one record of the type for the order, and returns them. */
    protected List<Published> awaitEvents(String topic, UUID orderId, String eventType) {
        List<List<Published>> holder = new ArrayList<>();
        holder.add(List.of());
        await().atMost(WAIT).pollInterval(POLL).until(() -> {
            holder.set(0, eventsOfType(topic, orderId, eventType));
            return !holder.get(0).isEmpty();
        });
        return holder.get(0);
    }

    // ------------------------------------------------------------------ HTTP helpers

    /** HTTP response with the body parsed as JSON (null when the body is empty). */
    public record Resp(int status, String contentType, JsonNode body) {
    }

    protected Resp http(String method, String path, String jsonBody) {
        try {
            HttpRequest.BodyPublisher body = jsonBody == null
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofString(jsonBody);
            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                    .timeout(Duration.ofSeconds(10))
                    .method(method, body);
            if (jsonBody != null) {
                builder.header("Content-Type", "application/json");
            }
            HttpResponse<String> response = HTTP.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            String text = response.body();
            JsonNode node = (text == null || text.isBlank()) ? null : JSON.readTree(text);
            return new Resp(response.statusCode(), response.headers().firstValue("Content-Type").orElse(""), node);
        } catch (Exception e) {
            throw new IllegalStateException(method + " " + path + " failed", e);
        }
    }

    protected Resp get(String path) {
        return http("GET", path, null);
    }

    protected Resp patchRestaurant(UUID id, String json) {
        return http("PATCH", "/restaurants/" + id, json);
    }

    protected Resp markReady(UUID orderId) {
        return http("POST", "/tickets/" + orderId + "/ready", null);
    }

    /** Current ticket status via GET /tickets/{orderId}, or null when the ticket does not exist. */
    protected String ticketStatus(UUID orderId) {
        Resp r = get("/tickets/" + orderId);
        if (r.status() == 404) {
            return null;
        }
        if (r.status() != 200) {
            throw new IllegalStateException("GET /tickets/" + orderId + " returned " + r.status());
        }
        return r.body().get("status").asText();
    }

    protected void awaitTicketStatus(UUID orderId, String expected) {
        await().atMost(WAIT).pollInterval(POLL).until(() -> expected.equals(ticketStatus(orderId)));
    }

    /** Asserts for 3 seconds that the ticket keeps the given status. */
    protected void assertTicketStaysAt(UUID orderId, String expected) {
        await().during(QUIET).atMost(WAIT).pollInterval(POLL)
                .until(() -> expected.equals(ticketStatus(orderId)));
    }
}
