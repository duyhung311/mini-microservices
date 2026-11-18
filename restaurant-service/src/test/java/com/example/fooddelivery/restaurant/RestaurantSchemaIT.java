package com.example.fooddelivery.restaurant;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.restaurant.support.RestaurantIntegrationTestBase;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

/** Criterion 13: Flyway creates exactly the spec section 9 restaurant-db tables and columns plus documented additions. */
class RestaurantSchemaIT extends RestaurantIntegrationTestBase {

    /** Spec section 9 columns plus the additive ones (kitchen_tickets.correlation_id, cause_event_id). */
    private static final Map<String, Set<String>> EXPECTED = Map.of(
            "restaurants", Set.of("id", "name", "address", "is_open", "max_concurrent_orders"),
            "menu_items", Set.of("id", "restaurant_id", "name", "price", "available"),
            "kitchen_tickets", Set.of("id", "order_id", "restaurant_id", "status", "created_at",
                    "correlation_id", "cause_event_id"),
            "outbox", Set.of("id", "aggregate_id", "event_type", "payload", "created_at", "published_at",
                    "traceparent"),
            "processed_events", Set.of("event_id", "processed_at"));

    private Map<String, Set<String>> actualColumns() {
        Map<String, Set<String>> actual = new TreeMap<>();
        jdbc.query("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'public'",
                rs -> {
                    actual.computeIfAbsent(rs.getString("table_name"), k -> new TreeSet<>())
                            .add(rs.getString("column_name"));
                });
        return actual;
    }

    private String dataType(String table, String column) {
        return jdbc.queryForObject("SELECT data_type FROM information_schema.columns "
                + "WHERE table_schema = 'public' AND table_name = ? AND column_name = ?", String.class, table, column);
    }

    private String nullable(String table, String column) {
        return jdbc.queryForObject("SELECT is_nullable FROM information_schema.columns "
                + "WHERE table_schema = 'public' AND table_name = ? AND column_name = ?", String.class, table, column);
    }

    private List<String> primaryKey(String table) {
        return jdbc.queryForList("SELECT kcu.column_name FROM information_schema.table_constraints tc "
                + "JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name "
                + "AND tc.table_schema = kcu.table_schema "
                + "WHERE tc.table_schema = 'public' AND tc.table_name = ? "
                + "AND tc.constraint_type = 'PRIMARY KEY'", String.class, table);
    }

    @Test
    void publicSchemaHoldsExactlyTheExpectedTablesAndColumns() {
        Map<String, Set<String>> actual = actualColumns();
        actual.remove("flyway_schema_history");

        assertThat(actual.keySet()).containsExactlyInAnyOrderElementsOf(EXPECTED.keySet());
        for (Map.Entry<String, Set<String>> table : EXPECTED.entrySet()) {
            assertThat(actual.get(table.getKey())).as("columns of %s", table.getKey())
                    .containsExactlyInAnyOrderElementsOf(table.getValue());
        }
    }

    @Test
    void outboxColumnTypesAndNullability() {
        assertThat(dataType("outbox", "id")).isEqualTo("bigint");
        assertThat(dataType("outbox", "aggregate_id")).isEqualTo("uuid");
        assertThat(dataType("outbox", "event_type")).isEqualTo("text");
        assertThat(dataType("outbox", "payload")).isEqualTo("jsonb");
        assertThat(dataType("outbox", "created_at")).isEqualTo("timestamp with time zone");
        assertThat(dataType("outbox", "published_at")).isEqualTo("timestamp with time zone");
        assertThat(nullable("outbox", "published_at")).isEqualTo("YES");
        for (String column : List.of("id", "aggregate_id", "event_type", "payload", "created_at")) {
            assertThat(nullable("outbox", column)).as("outbox.%s nullable", column).isEqualTo("NO");
        }
        assertThat(primaryKey("outbox")).containsExactly("id");
    }

    @Test
    void processedEventsHasEventIdAsPrimaryKey() {
        assertThat(dataType("processed_events", "event_id")).isEqualTo("uuid");
        assertThat(dataType("processed_events", "processed_at")).isEqualTo("timestamp with time zone");
        assertThat(primaryKey("processed_events")).containsExactly("event_id");
    }

    @Test
    void flywayHistoryExistsAndAllMigrationsSucceeded() {
        assertThat(actualColumns()).containsKey("flyway_schema_history");
        List<String> failed = jdbc.queryForList(
                "SELECT version FROM flyway_schema_history WHERE success = false", String.class);
        assertThat(failed).isEmpty();
        List<String> versions = jdbc.queryForList(
                "SELECT version FROM flyway_schema_history WHERE version IS NOT NULL", String.class);
        assertThat(versions).contains("1", "2", "3");
    }
}
