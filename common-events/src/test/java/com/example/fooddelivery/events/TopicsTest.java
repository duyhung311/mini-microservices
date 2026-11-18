package com.example.fooddelivery.events;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class TopicsTest {

    /** Hard-coded from the plan's topic table: name to partitions. */
    private static final Map<String, Integer> EXPECTED = expected();

    private static Map<String, Integer> expected() {
        Map<String, Integer> m = new LinkedHashMap<>();
        m.put("order.events", 3);
        m.put("restaurant.events", 3);
        m.put("delivery.events", 3);
        String[][] pairs = {
                {"order.events", "restaurant-service"},
                {"order.events", "delivery-service"},
                {"restaurant.events", "order-service"},
                {"restaurant.events", "delivery-service"},
                {"delivery.events", "order-service"},
                {"delivery.events", "restaurant-service"}};
        for (String[] p : pairs) {
            for (int i = 0; i < 3; i++) {
                m.put(p[0] + "." + p[1] + ".retry-" + i, 3);
            }
            m.put(p[0] + "." + p[1] + ".dlt", 1);
        }
        return m;
    }

    private static Map<String, Integer> actual() {
        Map<String, Integer> m = new LinkedHashMap<>();
        for (Topics.TopicSpec t : Topics.all()) {
            m.put(t.name(), t.partitions());
        }
        return m;
    }

    @Test
    void hardCodedExpectationHas27Entries() {
        assertThat(EXPECTED).hasSize(27);
    }

    @Test
    void allHasExactly27EntriesWithUniqueNames() {
        List<String> names = Topics.all().stream().map(Topics.TopicSpec::name).collect(Collectors.toList());

        assertThat(names).hasSize(27);
        assertThat(names).doesNotHaveDuplicates();
    }

    @Test
    void allMatchesTheExpectedNamesAndPartitions() {
        assertThat(actual()).isEqualTo(EXPECTED);
    }

    @Test
    void mainTopicsHaveThreePartitions() {
        Map<String, Integer> actual = actual();

        assertThat(actual).containsEntry("order.events", 3)
                .containsEntry("restaurant.events", 3)
                .containsEntry("delivery.events", 3);
    }

    @Test
    void retryTopicsHaveThreePartitionsAndDltsHaveOne() {
        Map<String, Integer> actual = actual();
        String[][] pairs = {
                {"order", "restaurant-service"}, {"order", "delivery-service"},
                {"restaurant", "order-service"}, {"restaurant", "delivery-service"},
                {"delivery", "order-service"}, {"delivery", "restaurant-service"}};
        for (String[] p : pairs) {
            String base = p[0] + ".events." + p[1];
            for (int i = 0; i < 3; i++) {
                assertThat(actual).as(base + ".retry-" + i).containsEntry(base + ".retry-" + i, 3);
            }
            assertThat(actual).as(base + ".dlt").containsEntry(base + ".dlt", 1);
        }
        assertThat(actual.values().stream().filter(p -> p == 3).count()).isEqualTo(21);
        assertThat(actual.values().stream().filter(p -> p == 1).count()).isEqualTo(6);
    }

    @Test
    void constantsUseTheBindingNames() {
        assertThat(Topics.ORDER_EVENTS).isEqualTo("order.events");
        assertThat(Topics.RESTAURANT_EVENTS).isEqualTo("restaurant.events");
        assertThat(Topics.DELIVERY_EVENTS).isEqualTo("delivery.events");
    }

    @Test
    void matchesComposeTopicListWhenPresent() throws IOException {
        Path file = Path.of("..", "infra", "kafka", "topics.txt").toAbsolutePath().normalize();
        assumeThat(Files.exists(file)).as("optional file %s is absent", file).isTrue();

        Map<String, Integer> fromFile = new LinkedHashMap<>();
        for (String line : Files.readAllLines(file)) {
            String t = line.trim();
            if (t.isEmpty() || t.startsWith("#")) {
                continue;
            }
            String[] parts = t.split("\\s+");
            fromFile.put(parts[0], Integer.parseInt(parts[1]));
        }

        assertThat(fromFile).isEqualTo(EXPECTED);
    }
}
