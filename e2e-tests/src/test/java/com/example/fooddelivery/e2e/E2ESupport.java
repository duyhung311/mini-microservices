package com.example.fooddelivery.e2e;

import static org.awaitility.Awaitility.await;

import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventEnvelope;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Shared helpers for the *E2E classes. Every test resets the shared state it depends on in
 * {@code @BeforeEach} (drivers AVAILABLE through JDBC, Pasta Place open with a large capacity through
 * PATCH) and restores it in {@code @AfterEach}.
 */
abstract class E2ESupport {

    static final ObjectMapper JSON = new ObjectMapper();
    static final Duration SAGA_TIMEOUT = Duration.ofSeconds(60);

    static final String ORDER_URL = System.getProperty("e2e.order-url", "http://localhost:8000");
    static final String RESTAURANT_URL = System.getProperty("e2e.restaurant-url", "http://localhost:8000");
    static final String DELIVERY_URL = System.getProperty("e2e.delivery-url", "http://localhost:8000");
    static final String EUREKA_URL = System.getProperty("e2e.eureka-url", "http://localhost:8761");
    static final String KAFKA = System.getProperty("e2e.kafka", "localhost:9092");
    static final String PG_HOST_PORT = System.getProperty("e2e.pg", "localhost:5433");

    static final String PASTA_PLACE = "11111111-1111-1111-1111-111111111111";
    static final String NIGHT_GRILL = "11111111-1111-1111-1111-111111111112";
    static final String MARGHERITA = "22222222-2222-2222-2222-000000000001";
    static final String TRUFFLE = "22222222-2222-2222-2222-000000000003";
    static final int DRIVER_COUNT = 5;
    static final int SEEDED_CAPACITY = 5;

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5)).build();

    record Resp(int status, String body) {
        JsonNode json() {
            try {
                return JSON.readTree(body);
            } catch (IOException e) {
                throw new IllegalStateException("not JSON: " + body, e);
            }
        }
    }

    @BeforeEach
    void resetSharedState() throws Exception {
        setDriversAvailable();
        patchRestaurant(PASTA_PLACE, "{\"open\":true,\"maxConcurrentOrders\":1000}");
    }

    @AfterEach
    void restoreSharedState() throws Exception {
        try {
            patchRestaurant(PASTA_PLACE, "{\"open\":true,\"maxConcurrentOrders\":" + SEEDED_CAPACITY + "}");
        } finally {
            setDriversAvailable();
        }
    }

    // ---- HTTP ----

    static Resp send(String method, String url, String body) {
        try {
            HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(15));
            if (body != null) {
                b.header("Content-Type", "application/json");
                b.method(method, HttpRequest.BodyPublishers.ofString(body));
            } else {
                b.method(method, HttpRequest.BodyPublishers.noBody());
            }
            HttpResponse<String> r = HTTP.send(b.build(), HttpResponse.BodyHandlers.ofString());
            return new Resp(r.statusCode(), r.body());
        } catch (IOException e) {
            throw new IllegalStateException(method + " " + url + " failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    static Resp get(String url) {
        return send("GET", url, null);
    }

    static Resp post(String url, String body) {
        return send("POST", url, body);
    }

    static void patchRestaurant(String id, String body) {
        Resp r = send("PATCH", RESTAURANT_URL + "/restaurants/" + id, body);
        if (r.status() != 200) {
            throw new IllegalStateException("PATCH restaurant failed: " + r.status() + " " + r.body());
        }
    }

    static Resp patchDriver(String id, String status) {
        return send("PATCH", DELIVERY_URL + "/drivers/" + id, "{\"status\":\"" + status + "\"}");
    }

    static String driverId(int n) {
        return "33333333-3333-3333-3333-00000000000" + n;
    }

    // ---- orders ----

    static String orderBody(String restaurantId, String menuItemId) {
        return "{\"customerId\":\"" + UUID.randomUUID() + "\",\"restaurantId\":\"" + restaurantId
                + "\",\"items\":[{\"menuItemId\":\"" + menuItemId
                + "\",\"quantity\":1,\"unitPrice\":12.50}],\"deliveryAddress\":\"x\"}";
    }

    static UUID placeOrder(String restaurantId, String menuItemId) {
        Resp r = post(ORDER_URL + "/orders", orderBody(restaurantId, menuItemId));
        if (r.status() != 202) {
            throw new IllegalStateException("POST /orders -> " + r.status() + " " + r.body());
        }
        return UUID.fromString(r.json().get("orderId").asText());
    }

    static String orderStatus(UUID id) {
        Resp r = get(ORDER_URL + "/orders/" + id);
        return r.status() == 200 ? r.json().get("status").asText() : "HTTP_" + r.status();
    }

    static String orderDriverId(UUID id) {
        return get(ORDER_URL + "/orders/" + id).json().path("driverId").asText();
    }

    static void awaitOrderStatus(UUID id, String expected) {
        await().atMost(SAGA_TIMEOUT).pollInterval(Duration.ofMillis(500)).untilAsserted(
                () -> Assertions.assertThat(orderStatus(id)).isEqualTo(expected));
    }

    static String ticketStatus(UUID id) {
        Resp r = get(RESTAURANT_URL + "/tickets/" + id);
        return r.status() == 200 ? r.json().get("status").asText() : "HTTP_" + r.status();
    }

    static String deliveryStatus(UUID id) {
        Resp r = get(DELIVERY_URL + "/deliveries/" + id);
        return r.status() == 200 ? r.json().get("status").asText() : "HTTP_" + r.status();
    }

    static String driverStatus(String driverId) {
        Resp r = get(DELIVERY_URL + "/drivers");
        for (JsonNode d : r.json()) {
            if (driverId.equals(d.get("id").asText())) {
                return d.get("status").asText();
            }
        }
        return "MISSING";
    }

    static List<String> history(UUID id) {
        Resp r = get(ORDER_URL + "/orders/" + id + "/history");
        List<String> out = new ArrayList<>();
        for (JsonNode h : r.json()) {
            out.add(h.get("toStatus").asText());
        }
        return out;
    }

    // ---- JDBC (state reset only) ----

    static void setDriversAvailable() throws SQLException {
        String url = "jdbc:postgresql://" + PG_HOST_PORT + "/delivery_db";
        try (Connection c = DriverManager.getConnection(url, "app", "app");
                Statement s = c.createStatement()) {
            s.executeUpdate("UPDATE drivers SET status='AVAILABLE'");
        }
    }

    // ---- Kafka ----

    /** Reads a topic from its current end, keeping the decoded envelopes seen so far. */
    static final class EventTap implements AutoCloseable {
        private final KafkaConsumer<String, String> consumer;
        private final List<ConsumerRecord<String, String>> seen = new ArrayList<>();

        EventTap(String topic) {
            Properties p = new Properties();
            p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA);
            p.put(ConsumerConfig.GROUP_ID_CONFIG, "e2e-tap-" + UUID.randomUUID());
            p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumer = new KafkaConsumer<>(p);
            List<TopicPartition> tps = new ArrayList<>();
            consumer.partitionsFor(topic).forEach(i -> tps.add(new TopicPartition(topic, i.partition())));
            consumer.assign(tps);
            consumer.seekToEnd(tps);
            for (TopicPartition tp : tps) {
                consumer.position(tp); // resolves the seek before the order is posted
            }
        }

        /** Polls once and returns the envelopes for the key seen so far (cumulative). */
        synchronized List<EventEnvelope> eventsFor(UUID orderId) {
            consumer.poll(Duration.ofMillis(300)).forEach(seen::add);
            List<EventEnvelope> out = new ArrayList<>();
            for (ConsumerRecord<String, String> r : seen) {
                if (orderId.toString().equals(r.key())) {
                    out.add(EventCodec.fromJson(r.value()));
                }
            }
            return Collections.unmodifiableList(out);
        }

        List<String> typesFor(UUID orderId) {
            List<String> t = new ArrayList<>();
            eventsFor(orderId).forEach(e -> t.add(e.eventType()));
            return t;
        }

        @Override
        public void close() {
            consumer.close(Duration.ofSeconds(5));
        }
    }
}
