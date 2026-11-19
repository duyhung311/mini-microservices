package com.example.fooddelivery.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/** Criterion 29. Prometheus targets, consumer-group lag, custom metrics and the provisioned Grafana dashboard. */
class ObservabilityE2E {

    static final String PROMETHEUS_URL = System.getProperty("e2e.prometheus-url", "http://localhost:9090");
    static final String GRAFANA_URL = System.getProperty("e2e.grafana-url", "http://localhost:3000");
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    static String http(String url, String basicAuth) {
        try {
            HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(15));
            if (basicAuth != null) {
                b.header("Authorization", "Basic "
                        + Base64.getEncoder().encodeToString(basicAuth.getBytes(StandardCharsets.UTF_8)));
            }
            HttpResponse<String> r = HTTP.send(b.GET().build(), HttpResponse.BodyHandlers.ofString());
            assertThat(r.statusCode()).as("GET %s", url).isEqualTo(200);
            return r.body();
        } catch (IOException e) {
            throw new IllegalStateException("GET " + url + " failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    static JsonNode json(String body) {
        try {
            return E2ESupport.JSON.readTree(body);
        } catch (IOException e) {
            throw new IllegalStateException("not JSON: " + body, e);
        }
    }

    static JsonNode query(String promql) {
        String url = PROMETHEUS_URL + "/api/v1/query?query=" + URLEncoder.encode(promql, StandardCharsets.UTF_8);
        JsonNode j = json(http(url, null));
        assertThat(j.get("status").asText()).isEqualTo("success");
        return j.get("data").get("result");
    }

    /** application label if present, else the Eureka app name lower-cased, else the job. */
    static String nameOf(JsonNode labels) {
        if (labels.hasNonNull("application")) {
            return labels.get("application").asText();
        }
        if (labels.hasNonNull("eureka_app")) {
            return labels.get("eureka_app").asText().toLowerCase();
        }
        return labels.path("job").asText();
    }

    @Test
    void everyPrometheusTargetIsUpWithExpectedInstanceCounts() {
        Map<String, Integer> expected = new TreeMap<>(Map.of(
                "order-service", 1, "restaurant-service", 3, "delivery-service", 1, "api-gateway", 1,
                "discovery-server", 1, "kafka-exporter", 1));
        Awaitility.await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(2)).untilAsserted(() -> {
            JsonNode targets = json(http(PROMETHEUS_URL + "/api/v1/targets?state=active", null))
                    .get("data").get("activeTargets");
            Map<String, Integer> actual = new TreeMap<>();
            for (JsonNode t : targets) {
                assertThat(t.get("health").asText()).as("target %s", t.get("scrapeUrl").asText()).isEqualTo("up");
                actual.merge(nameOf(t.get("labels")), 1, Integer::sum);
            }
            assertThat(actual).isEqualTo(expected);
        });
    }

    @Test
    void consumerGroupLagHasSeriesForAllThreeGroups() {
        Awaitility.await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(2)).untilAsserted(() -> {
            Set<String> groups = new HashSet<>();
            for (JsonNode s : query("kafka_consumergroup_lag")) {
                groups.add(s.get("metric").path("consumergroup").asText());
            }
            assertThat(groups).contains("order-service", "restaurant-service", "delivery-service");
        });
    }

    @Test
    void grafanaDashboardIsProvisionedWithLagOutboxAndDltPanels() {
        String body = http(GRAFANA_URL + "/api/dashboards/uid/saga-overview", "admin:admin");
        JsonNode dash = json(body).get("dashboard");
        assertThat(dash.get("uid").asText()).isEqualTo("saga-overview");
        assertThat(dash.get("panels").size()).isGreaterThan(0);
        StringBuilder exprs = new StringBuilder();
        for (JsonNode panel : dash.get("panels")) {
            for (JsonNode target : panel.path("targets")) {
                exprs.append(target.path("expr").asText()).append('\n');
            }
        }
        assertThat(exprs.toString()).contains("kafka_consumergroup_lag").contains("outbox_pending")
                .containsIgnoringCase("dlt");
    }

    /** /actuator/prometheus is not exposed through the gateway, so the custom metrics are read via Prometheus. */
    @Test
    void customMetricsAreScrapedByPrometheus() {
        Awaitility.await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(2)).untilAsserted(() -> {
            assertThat(query("outbox_pending").size()).as("outbox_pending series").isGreaterThan(0);
            assertThat(query("restaurant_orders_handled_total").size()).as("restaurant_orders_handled_total series")
                    .isGreaterThan(0);
        });
    }
}
