package com.example.fooddelivery.delivery.support;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.fooddelivery.events.CancelReason;
import com.example.fooddelivery.events.Envelopes;
import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventHeaders;
import com.example.fooddelivery.events.EventPayload;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.OrderAccepted;
import com.example.fooddelivery.events.OrderCancelled;
import com.example.fooddelivery.events.OrderPlaced;
import com.example.fooddelivery.events.OrderReady;
import com.example.fooddelivery.events.OrderRejected;
import com.example.fooddelivery.events.RejectReason;
import com.example.fooddelivery.events.Topics;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
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
 * Support base class for the delivery-service integration tests: static singleton Kafka and Postgres
 * containers (same image tags as Compose), all 27 topics created once from {@link Topics#all()},
 * RANDOM_PORT web server, producer/consumer helpers, a small HTTP client and a state reset.
 *
 * <p>The 5 seeded drivers are shared by all tests, so before and after every test the active deliveries
 * are closed first and only then every driver is set AVAILABLE (resetting only the drivers would leave
 * stale active deliveries behind).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class DeliveryIntegrationTestBase {

    protected static final UUID DRIVER_1 = UUID.fromString("33333333-3333-3333-3333-000000000001");
    protected static final UUID DRIVER_2 = UUID.fromString("33333333-3333-3333-3333-000000000002");
    protected static final UUID RESTAURANT = UUID.fromString("11111111-1111-1111-1111-111111111111");

    protected static final String PICKUP_ADDRESS = "1 Kitchen St";
    protected static final String DROPOFF_ADDRESS = "99 Customer Road";

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
    @AfterEach
    protected void resetSharedState() {
        jdbc.update("UPDATE deliveries SET status = 'CANCELLED', updated_at = now() "
                + "WHERE status IN ('ASSIGNED', 'READY_FOR_PICKUP', 'PICKED_UP')");
        jdbc.update("UPDATE drivers SET status = 'AVAILABLE'");
    }

    protected void setAllDrivers(String status) {
        jdbc.update("UPDATE drivers SET status = ?", status);
    }

    protected void setDriver(UUID id, String status) {
        jdbc.update("UPDATE drivers SET status = ? WHERE id = ?", status, id);
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

    private static OrderPlaced placedPayload() {
        return new OrderPlaced(UUID.randomUUID(), RESTAURANT,
                List.of(new OrderPlaced.Item(UUID.randomUUID(), 1, new BigDecimal("12.50"))),
                new BigDecimal("12.50"), DROPOFF_ADDRESS);
    }

    protected EventEnvelope sendPlaced(UUID orderId, UUID correlationId) {
        EventEnvelope envelope = Envelopes.first(EventTypes.ORDER_PLACED, orderId, correlationId, placedPayload());
        publish(Topics.ORDER_EVENTS, envelope);
        return envelope;
    }

    /** An OrderPlaced envelope that is never published; used only as the cause of another event. */
    protected EventEnvelope unpublishedPlaced(UUID orderId, UUID correlationId) {
        return Envelopes.first(EventTypes.ORDER_PLACED, orderId, correlationId, placedPayload());
    }

    /** OrderAccepted caused by {@code cause} (causationId = cause.eventId, same correlationId). */
    protected EventEnvelope sendAccepted(EventEnvelope cause) {
        EventEnvelope envelope = Envelopes.next(cause, EventTypes.ORDER_ACCEPTED,
                new OrderAccepted(RESTAURANT, 15, PICKUP_ADDRESS));
        publish(Topics.RESTAURANT_EVENTS, envelope);
        return envelope;
    }

    /** OrderAccepted with no OrderPlaced before it. */
    protected EventEnvelope sendAcceptedAlone(UUID orderId, UUID correlationId) {
        EventEnvelope envelope = Envelopes.first(EventTypes.ORDER_ACCEPTED, orderId, correlationId,
                new OrderAccepted(RESTAURANT, 15, PICKUP_ADDRESS));
        publish(Topics.RESTAURANT_EVENTS, envelope);
        return envelope;
    }

    protected EventEnvelope sendReady(EventEnvelope cause) {
        EventEnvelope envelope = Envelopes.next(cause, EventTypes.ORDER_READY,
                new OrderReady(RESTAURANT, Instant.now()));
        publish(Topics.RESTAURANT_EVENTS, envelope);
        return envelope;
    }

    protected EventEnvelope sendRejected(EventEnvelope cause) {
        EventEnvelope envelope = Envelopes.next(cause, EventTypes.ORDER_REJECTED,
                new OrderRejected(RESTAURANT, RejectReason.CLOSED));
        publish(Topics.RESTAURANT_EVENTS, envelope);
        return envelope;
    }

    protected EventEnvelope sendCancelled(EventEnvelope cause) {
        EventEnvelope envelope = Envelopes.next(cause, EventTypes.ORDER_CANCELLED,
                new OrderCancelled(CancelReason.CUSTOMER_REQUEST));
        publish(Topics.ORDER_EVENTS, envelope);
        return envelope;
    }

    protected EventEnvelope sendCancelledAlone(UUID orderId, UUID correlationId) {
        EventEnvelope envelope = Envelopes.first(EventTypes.ORDER_CANCELLED, orderId, correlationId,
                new OrderCancelled(CancelReason.CUSTOMER_REQUEST));
        publish(Topics.ORDER_EVENTS, envelope);
        return envelope;
    }

    /** One record read from a topic. */
    public record Published(String key, int partition, Map<String, String> headers, EventEnvelope envelope) {
        public String type() {
            return envelope.eventType();
        }

        public EventPayload payload() {
            return EventCodec.payloadOf(envelope);
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
                            headers.put(h.key(), new String(h.value(), StandardCharsets.UTF_8));
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

    protected List<Published> deliveryEvents(UUID orderId, String eventType) {
        return eventsFor(Topics.DELIVERY_EVENTS, orderId).stream().filter(p -> p.type().equals(eventType))
                .collect(Collectors.toList());
    }

    /** Waits (max 30 s) until delivery.events holds at least one record of the type for the order. */
    protected List<Published> awaitDeliveryEvents(UUID orderId, String eventType) {
        List<List<Published>> holder = new ArrayList<>();
        holder.add(List.of());
        await().atMost(WAIT).pollInterval(POLL).until(() -> {
            holder.set(0, deliveryEvents(orderId, eventType));
            return !holder.get(0).isEmpty();
        });
        return holder.get(0);
    }

    /** Asserts for 3 seconds that delivery.events never holds a record of the type for the order. */
    protected void assertNoDeliveryEvent(UUID orderId, String eventType) {
        await().during(QUIET).atMost(WAIT).pollInterval(POLL)
                .until(() -> deliveryEvents(orderId, eventType).isEmpty());
    }

    /**
     * Key = orderId, headers eventType/eventVersion/correlationId equal the envelope values,
     * eventVersion 1, and the expected correlation and causation ids (causation may be null).
     */
    protected void assertEnvelope(Published p, String type, UUID orderId, UUID correlationId, UUID causationId) {
        assertThat(p.key()).isEqualTo(orderId.toString());
        assertThat(p.envelope().orderId()).isEqualTo(orderId);
        assertThat(p.envelope().eventType()).isEqualTo(type);
        assertThat(p.envelope().eventVersion()).isEqualTo(1);
        assertThat(p.envelope().correlationId()).isEqualTo(correlationId);
        assertThat(p.envelope().causationId()).isEqualTo(causationId);
        assertThat(p.headers().get(EventHeaders.EVENT_TYPE)).isEqualTo(type);
        assertThat(p.headers().get(EventHeaders.EVENT_VERSION)).isEqualTo("1");
        assertThat(p.headers().get(EventHeaders.CORRELATION_ID)).isEqualTo(correlationId.toString());
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

    protected Resp patchDriver(UUID id, String json) {
        return http("PATCH", "/drivers/" + id, json);
    }

    protected Resp patchDriverStatus(UUID id, String status) {
        return patchDriver(id, "{\"status\":\"" + status + "\"}");
    }

    protected Resp pickup(UUID orderId) {
        return http("POST", "/deliveries/" + orderId + "/pickup", null);
    }

    protected Resp deliver(UUID orderId) {
        return http("POST", "/deliveries/" + orderId + "/deliver", null);
    }

    /** Current delivery status via GET /deliveries/{orderId}, or null when the delivery does not exist. */
    protected String deliveryStatus(UUID orderId) {
        Resp r = get("/deliveries/" + orderId);
        if (r.status() == 404) {
            return null;
        }
        if (r.status() != 200) {
            throw new IllegalStateException("GET /deliveries/" + orderId + " returned " + r.status());
        }
        return r.body().get("status").asText();
    }

    protected void awaitDeliveryStatus(UUID orderId, String expected) {
        await().atMost(WAIT).pollInterval(POLL).until(() -> expected.equals(deliveryStatus(orderId)));
    }

    /** Asserts for 3 seconds that the delivery keeps the given status. */
    protected void assertDeliveryStaysAt(UUID orderId, String expected) {
        await().during(QUIET).atMost(WAIT).pollInterval(POLL)
                .until(() -> expected.equals(deliveryStatus(orderId)));
    }

    /** Status of one driver via GET /drivers, or null if the driver is not listed. */
    protected String driverStatus(UUID driverId) {
        Resp r = get("/drivers");
        for (JsonNode d : r.body()) {
            if (driverId.toString().equals(d.get("id").asText())) {
                return d.get("status").asText();
            }
        }
        return null;
    }

    protected void awaitDriverStatus(UUID driverId, String expected) {
        await().atMost(WAIT).pollInterval(POLL).until(() -> expected.equals(driverStatus(driverId)));
    }

    protected String driverName(UUID driverId) {
        for (JsonNode d : get("/drivers").body()) {
            if (driverId.toString().equals(d.get("id").asText())) {
                return d.get("name").asText();
            }
        }
        return null;
    }

    protected UUID deliveryDriverId(UUID orderId) {
        JsonNode d = get("/deliveries/" + orderId).body().get("driverId");
        return d == null || d.isNull() ? null : UUID.fromString(d.asText());
    }

    protected long countDriversWithStatus(String status) {
        Long n = jdbc.queryForObject("SELECT count(*) FROM drivers WHERE status = ?", Long.class, status);
        return n == null ? 0 : n;
    }

    /** Places and accepts a new order, waits until it is ASSIGNED and returns {placed, accepted}. */
    protected EventEnvelope[] assignedOrder(UUID orderId, UUID correlationId) {
        EventEnvelope placed = sendPlaced(orderId, correlationId);
        EventEnvelope accepted = sendAccepted(placed);
        awaitDeliveryStatus(orderId, "ASSIGNED");
        return new EventEnvelope[] {placed, accepted};
    }
}
