package com.example.fooddelivery.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/**
 * Criterion 30 (trace part). The happy-path saga is one Jaeger trace whose traceId is the correlationId (hex, no
 * dashes) with spans from all four services.
 *
 * <p>correlationId tag decision: the tag is asserted on the three Kafka services (order, restaurant, delivery) and
 * not on api-gateway. The gateway starts the trace before the order service has generated the correlationId, so a
 * gateway span cannot know it; the three Kafka services handle envelopes that carry it.
 */
class SagaTracingE2E extends E2ESupport {

    static final String JAEGER_URL = System.getProperty("e2e.jaeger-url", "http://localhost:16686");
    static final String RELAY_SPAN = "outbox relay publish";
    static final Set<String> ALL_SERVICES =
            Set.of("api-gateway", "order-service", "restaurant-service", "delivery-service");
    static final Set<String> KAFKA_SERVICES = Set.of("order-service", "restaurant-service", "delivery-service");

    @Test
    void happyPathIsOneTraceAcrossAllServicesWithCorrelationIdAsTraceId() {
        Resp placed = post(ORDER_URL + "/orders", orderBody(PASTA_PLACE, MARGHERITA));
        assertThat(placed.status()).isEqualTo(202);
        UUID id = UUID.fromString(placed.json().get("orderId").asText());
        String correlationId = placed.json().get("correlationId").asText();
        String hex = correlationId.replace("-", "");
        assertThat(hex).hasSize(32);

        awaitOrderStatus(id, "DRIVER_ASSIGNED");
        assertThat(post(RESTAURANT_URL + "/tickets/" + id + "/ready", null).status()).isEqualTo(200);
        awaitOrderStatus(id, "READY_FOR_PICKUP");
        assertThat(post(DELIVERY_URL + "/deliveries/" + id + "/pickup", null).status()).isEqualTo(200);
        awaitOrderStatus(id, "OUT_FOR_DELIVERY");
        assertThat(post(DELIVERY_URL + "/deliveries/" + id + "/deliver", null).status()).isEqualTo(200);
        awaitOrderStatus(id, "DELIVERED");

        Awaitility.await().atMost(SAGA_TIMEOUT).pollInterval(Duration.ofSeconds(2)).untilAsserted(() -> {
            JsonNode data = fetchTrace(hex);
            assertThat(data.size()).as("traces for %s", hex).isEqualTo(1);
            assertThat(servicesOf(data.get(0)).values()).containsAll(ALL_SERVICES);
            // spans are exported late, so wait for the relay spans of all three Kafka services too
            assertThat(relayServices(data.get(0))).containsAll(KAFKA_SERVICES);
        });

        JsonNode trace = fetchTrace(hex).get(0);
        assertThat(trace.get("traceID").asText()).isEqualTo(hex);

        Map<String, String> services = servicesOf(trace);
        Set<String> relayServices = new HashSet<>();
        Map<String, Set<String>> correlationTagged = new HashMap<>();
        for (JsonNode span : trace.get("spans")) {
            String service = services.get(span.get("processID").asText());
            assertThat(span.get("traceID").asText()).isEqualTo(hex);
            if (RELAY_SPAN.equals(span.get("operationName").asText())) {
                relayServices.add(service);
                assertThat(span.path("references").size())
                        .as("relay span in %s must have a parent reference", service).isGreaterThanOrEqualTo(1);
                assertThat(span.get("references").get(0).get("spanID").asText()).isNotEmpty();
            }
            for (JsonNode tag : span.path("tags")) {
                if ("correlationId".equals(tag.get("key").asText())
                        && correlationId.equals(tag.get("value").asText())) {
                    correlationTagged.computeIfAbsent(service, k -> new HashSet<>()).add(span.get("spanID").asText());
                }
            }
        }
        assertThat(relayServices).containsAll(KAFKA_SERVICES);
        assertThat(correlationTagged.keySet()).containsAll(KAFKA_SERVICES);
    }

    static JsonNode fetchTrace(String hex) {
        Resp r = get(JAEGER_URL + "/api/traces/" + hex);
        assertThat(r.status()).as("GET trace %s", hex).isEqualTo(200);
        return r.json().path("data");
    }

    static Map<String, String> servicesOf(JsonNode trace) {
        Map<String, String> out = new HashMap<>();
        trace.get("processes").fields().forEachRemaining(
                e -> out.put(e.getKey(), e.getValue().get("serviceName").asText()));
        return out;
    }

    static Set<String> relayServices(JsonNode trace) {
        Map<String, String> services = servicesOf(trace);
        Set<String> out = new HashSet<>();
        for (JsonNode span : trace.get("spans")) {
            if (RELAY_SPAN.equals(span.get("operationName").asText())) {
                out.add(services.get(span.get("processID").asText()));
            }
        }
        return out;
    }
}
