package com.example.fooddelivery.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/** Criteria 26 and 27. */
class DiscoveryE2E extends E2ESupport {

    /** Eureka renders a single element as an object and several as an array. */
    private static List<JsonNode> listOf(JsonNode n) {
        List<JsonNode> out = new ArrayList<>();
        if (n == null || n.isMissingNode() || n.isNull()) {
            return out;
        }
        if (n.isArray()) {
            n.forEach(out::add);
        } else {
            out.add(n);
        }
        return out;
    }

    /** App name to number of instances; an instance that is not UP makes the count -1. */
    private static Map<String, Integer> upCounts() throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(EUREKA_URL + "/eureka/apps"))
                .header("Accept", "application/json").timeout(Duration.ofSeconds(10)).build();
        HttpResponse<String> r = HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.ofString());
        assertThat(r.statusCode()).isEqualTo(200);
        JsonNode apps = JSON.readTree(r.body()).path("applications").path("application");
        Map<String, Integer> counts = new HashMap<>();
        for (JsonNode app : listOf(apps)) {
            int up = 0;
            int total = 0;
            for (JsonNode inst : listOf(app.path("instance"))) {
                total++;
                if ("UP".equals(inst.path("status").asText())) {
                    up++;
                }
            }
            counts.put(app.path("name").asText(), up == total ? up : -1);
        }
        return counts;
    }

    @Test
    void eurekaListsAllInstancesUp() {
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            Map<String, Integer> c = upCounts();
            assertThat(c.get("ORDER-SERVICE")).isEqualTo(1);
            assertThat(c.get("RESTAURANT-SERVICE")).isEqualTo(3);
            assertThat(c.get("DELIVERY-SERVICE")).isEqualTo(1);
            assertThat(c.get("API-GATEWAY")).isEqualTo(1);
        });
    }

    @Test
    void gatewayLoadBalancesRestaurantInstances() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < 12; i++) {
            HttpRequest req = HttpRequest.newBuilder(URI.create(RESTAURANT_URL + "/restaurants"))
                    .timeout(Duration.ofSeconds(10)).build();
            HttpResponse<String> r = client.send(req, HttpResponse.BodyHandlers.ofString());
            assertThat(r.statusCode()).isEqualTo(200);
            r.headers().firstValue("X-Instance-Id").ifPresent(ids::add);
        }
        assertThat(ids).hasSizeGreaterThanOrEqualTo(2);
    }
}
