package com.example.fooddelivery.restaurant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.Envelopes;
import com.example.fooddelivery.events.OrderCancelled;
import com.example.fooddelivery.events.CancelReason;
import com.example.fooddelivery.events.Topics;
import com.example.fooddelivery.restaurant.scaling.InstanceId;
import com.example.fooddelivery.restaurant.support.LiveContexts;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

import com.example.fooddelivery.restaurant.support.RestaurantIntegrationTestBase;

/**
 * Criterion 25 (M8) and the X-Instance-Id part of criterion 27: three restaurant instances (r1..r3) in the same
 * consumer group, same database, one {@code order.events} partition each. The shared test context and any other
 * cached context have their listeners stopped for the duration so that exactly these three members are in the group.
 */
class RestaurantScalingIT extends RestaurantIntegrationTestBase {

    private static final String GROUP = "restaurant-service";
    private static final Duration STABLE_WAIT = Duration.ofSeconds(60);
    private static final Duration RUN_WAIT = Duration.ofSeconds(60);
    private static final HttpClient HTTP_CLIENT =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    private static final List<ConfigurableApplicationContext> INSTANCES = new ArrayList<>();
    private static List<KafkaListenerEndpointRegistry> stopped = List.of();
    private static String bootstrap;

    @Autowired
    private Environment env;

    @AfterAll
    static void tearDownCluster() {
        for (ConfigurableApplicationContext ctx : INSTANCES) {
            try {
                ctx.close();
            } catch (RuntimeException ignored) {
                // closing must not hide the test result
            }
        }
        INSTANCES.clear();
        LiveContexts.restart(stopped);
        stopped = List.of();
    }

    // ------------------------------------------------------------------ cluster setup

    private void ensureCluster() {
        if (!INSTANCES.isEmpty()) {
            awaitOnePartitionEach();
            return;
        }
        bootstrap = env.getRequiredProperty("spring.kafka.bootstrap-servers");
        // null = stop every live context, including the shared test context of this class
        stopped = LiveContexts.stopListenersExcept(null);
        for (int i = 1; i <= 3; i++) {
            INSTANCES.add(new SpringApplicationBuilder(RestaurantApplication.class).run(
                    "--server.port=0",
                    "--app.instance-id=r" + i,
                    "--app.kafka.listener.concurrency=1",
                    "--spring.datasource.url=" + env.getRequiredProperty("spring.datasource.url"),
                    "--spring.datasource.username=" + env.getRequiredProperty("spring.datasource.username"),
                    "--spring.datasource.password=" + env.getRequiredProperty("spring.datasource.password"),
                    "--spring.kafka.bootstrap-servers=" + bootstrap));
        }
        awaitOnePartitionEach();
    }

    /** Waits (max 60 s) for a stable group where 3 consumers each own exactly one order.events partition. */
    private void awaitOnePartitionEach() {
        try (AdminClient admin = AdminClient.create(Map.<String, Object>of("bootstrap.servers", bootstrap))) {
            await().atMost(STABLE_WAIT).pollInterval(Duration.ofMillis(500)).until(() -> {
                ConsumerGroupDescription d = admin.describeConsumerGroups(List.of(GROUP))
                        .describedGroups().get(GROUP).get(10, TimeUnit.SECONDS);
                return oneOrderPartitionEach(d);
            });
        }
    }

    /**
     * Each instance runs two listener containers (order.events and delivery.events) in the same group, so the
     * group has 6 consumers. The order.events subscribers must be exactly 3, with one distinct partition each.
     */
    private static boolean oneOrderPartitionEach(ConsumerGroupDescription d) {
        if (!"Stable".equalsIgnoreCase(d.state().toString())) {
            return false;
        }
        List<TopicPartition> all = new ArrayList<>();
        for (MemberDescription m : d.members()) {
            List<TopicPartition> mine = m.assignment().topicPartitions().stream()
                    .filter(tp -> tp.topic().equals(Topics.ORDER_EVENTS)).toList();
            if (mine.size() > 1) {
                return false;
            }
            all.addAll(mine);
        }
        return all.size() == 3 && all.stream().map(TopicPartition::partition).distinct().count() == 3
                && d.members().size() == 6;
    }

    private static double handled(ConfigurableApplicationContext ctx) {
        double sum = 0;
        for (Counter c : ctx.getBean(MeterRegistry.class).find("restaurant.orders.handled").counters()) {
            sum += c.count();
        }
        return sum;
    }

    // ------------------------------------------------------------------ restaurant.events reader

    /** Reads restaurant.events from the beginning, returns eventType list per record key. */
    private Map<String, List<String>> decisionsByKey() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "t19-reader-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        Map<String, List<String>> out = new HashMap<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = consumer.partitionsFor(Topics.RESTAURANT_EVENTS).stream()
                    .map(p -> new TopicPartition(Topics.RESTAURANT_EVENTS, p.partition())).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (System.nanoTime() < deadline && !caughtUp(consumer, end)) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(200))) {
                    out.computeIfAbsent(r.key(), k -> new ArrayList<>())
                            .add(EventCodec.fromJson(r.value()).eventType());
                }
            }
        }
        return out;
    }

    private static boolean caughtUp(KafkaConsumer<String, String> consumer, Map<TopicPartition, Long> end) {
        for (Map.Entry<TopicPartition, Long> e : end.entrySet()) {
            if (consumer.position(e.getKey()) < e.getValue()) {
                return false;
            }
        }
        return true;
    }

    private static boolean isDecision(String type) {
        return EventTypes.ORDER_ACCEPTED.equals(type) || EventTypes.ORDER_REJECTED.equals(type);
    }

    private long countDecisions(Map<String, List<String>> all, UUID orderId) {
        return all.getOrDefault(orderId.toString(), List.of()).stream().filter(RestaurantScalingIT::isDecision).count();
    }

    // ------------------------------------------------------------------ tests

    /** Criterion 25: 3 members, one partition each (also asserted inside ensureCluster). */
    @Test
    void threeInstancesEachOwnExactlyOneOrderEventsPartition() throws Exception {
        ensureCluster();
        assertThat(INSTANCES).hasSize(3);
        try (AdminClient admin = AdminClient.create(Map.<String, Object>of("bootstrap.servers", bootstrap))) {
            ConsumerGroupDescription d = admin.describeConsumerGroups(List.of(GROUP))
                    .describedGroups().get(GROUP).get(10, TimeUnit.SECONDS);
            assertThat(oneOrderPartitionEach(d)).as("group description: %s", d).isTrue();
            List<Integer> partitions = new ArrayList<>();
            for (MemberDescription m : d.members()) {
                m.assignment().topicPartitions().stream().filter(tp -> tp.topic().equals(Topics.ORDER_EVENTS))
                        .forEach(tp -> partitions.add(tp.partition()));
            }
            assertThat(partitions).containsExactlyInAnyOrder(0, 1, 2);
        }
    }

    /** Criterion 25: every order gets exactly one decision and each instance handles at least one order. */
    @Test
    void everyInstanceHandlesOrdersAndEveryOrderGetsExactlyOneDecision() {
        ensureCluster();
        jdbc.update("UPDATE restaurants SET max_concurrent_orders = 1000 WHERE id = ?", PASTA_PLACE);
        try {
            double[] before = new double[3];
            for (int i = 0; i < 3; i++) {
                before[i] = handled(INSTANCES.get(i));
            }
            List<UUID> orders = new ArrayList<>();
            for (int i = 0; i < 36; i++) {
                orders.add(placeOrder(PASTA_PLACE, MARGHERITA).orderId());
            }
            await().atMost(RUN_WAIT).pollInterval(Duration.ofSeconds(1)).until(() -> {
                Map<String, List<String>> all = decisionsByKey();
                return orders.stream().allMatch(o -> countDecisions(all, o) >= 1);
            });
            // no duplicate decisions appear afterwards
            await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofSeconds(1))
                    .until(() -> {
                        Map<String, List<String>> all = decisionsByKey();
                        return orders.stream().allMatch(o -> countDecisions(all, o) == 1);
                    });
            for (int i = 0; i < 3; i++) {
                final int idx = i;
                await().atMost(Duration.ofSeconds(30)).pollInterval(POLL)
                        .until(() -> handled(INSTANCES.get(idx)) - before[idx] >= 1);
            }
            double total = 0;
            for (int i = 0; i < 3; i++) {
                total += handled(INSTANCES.get(i)) - before[i];
            }
            assertThat(total).isGreaterThanOrEqualTo(36);
        } finally {
            jdbc.update("UPDATE restaurants SET max_concurrent_orders = 5 WHERE id = ?", PASTA_PLACE);
        }
    }

    /** Criterion 25: OrderPlaced then OrderCancelled with the same key is processed in order on every instance. */
    @Test
    void placedThenCancelledIsProcessedInOrderForEveryOrder() {
        ensureCluster();
        jdbc.update("UPDATE restaurants SET max_concurrent_orders = 1000 WHERE id = ?", PASTA_PLACE);
        try {
            Map<UUID, EventEnvelope> placed = new LinkedHashMap<>();
            for (int i = 0; i < 15; i++) {
                EventEnvelope p = placeOrder(PASTA_PLACE, MARGHERITA);
                publish(Topics.ORDER_EVENTS, Envelopes.next(p, EventTypes.ORDER_CANCELLED,
                        new OrderCancelled(CancelReason.CUSTOMER_REQUEST)));
                placed.put(p.orderId(), p);
            }
            for (UUID orderId : placed.keySet()) {
                awaitTicketStatus(orderId, "CANCELLED");
            }
            await().during(QUIET).atMost(Duration.ofSeconds(30)).pollInterval(POLL).until(() -> {
                for (UUID orderId : placed.keySet()) {
                    if (!"CANCELLED".equals(ticketStatus(orderId))) {
                        return false;
                    }
                }
                return true;
            });
            for (UUID orderId : placed.keySet()) {
                Long tickets = jdbc.queryForObject("SELECT count(*) FROM kitchen_tickets WHERE order_id = ?",
                        Long.class, orderId);
                Long accepted = jdbc.queryForObject(
                        "SELECT count(*) FROM kitchen_tickets WHERE order_id = ? AND status = 'ACCEPTED'",
                        Long.class, orderId);
                assertThat(tickets).as("tickets for %s", orderId).isEqualTo(1L);
                assertThat(accepted).as("stray ACCEPTED ticket for %s", orderId).isZero();
            }
            assertThat(acceptedTickets(PASTA_PLACE)).isZero();
        } finally {
            jdbc.update("UPDATE restaurants SET max_concurrent_orders = 5 WHERE id = ?", PASTA_PLACE);
        }
    }

    /** Criterion 27 (X-Instance-Id part): each context answers with its own instance id. */
    @Test
    void eachInstanceAnswersWithItsOwnInstanceIdHeader() throws Exception {
        ensureCluster();
        for (int i = 0; i < 3; i++) {
            ConfigurableApplicationContext ctx = INSTANCES.get(i);
            String expected = "r" + (i + 1);
            assertThat(ctx.getBean(InstanceId.class).value()).isEqualTo(expected);
            String port = ctx.getEnvironment().getProperty("local.server.port");
            assertThat(port).isNotNull();
            HttpResponse<String> r = HTTP_CLIENT.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/restaurants"))
                            .timeout(Duration.ofSeconds(10)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(r.statusCode()).isEqualTo(200);
            assertThat(r.headers().firstValue("X-Instance-Id")).contains(expected);
        }
    }
}
