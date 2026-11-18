package com.example.fooddelivery.order;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Criterion 13: Flyway creates exactly the spec section 9 tables and columns of order-db plus the documented additive
 * ones (orders.correlation_id; outbox.traceparent is allowed as the later M10 addition). The app booting at all proves
 * ddl-auto=validate.
 */
class OrderSchemaIT extends AbstractOrderIT {

    @Autowired
    private JdbcTemplate jdbc;

    private static Set<String> set(String... values) {
        return new TreeSet<>(Arrays.asList(values));
    }

    private Map<String, Set<String>> actualColumns() {
        Map<String, Set<String>> out = new TreeMap<>();
        jdbc.query("select table_name, column_name from information_schema.columns where table_schema = 'public'",
                rs -> {
                    out.computeIfAbsent(rs.getString(1), k -> new TreeSet<>()).add(rs.getString(2));
                });
        return out;
    }

    @Test
    void tablesAndColumnsMatchSpecPlusAdditiveOnes() {
        Map<String, Set<String>> expected = new TreeMap<>();
        // spec 9: orders (...) plus additive correlation_id
        expected.put("orders", set("id", "customer_id", "restaurant_id", "status", "total_amount",
                "delivery_address", "driver_id", "created_at", "updated_at", "version", "correlation_id"));
        expected.put("order_items", set("id", "order_id", "menu_item_id", "quantity", "unit_price"));
        expected.put("order_status_history",
                set("id", "order_id", "from_status", "to_status", "event_id", "changed_at"));
        expected.put("outbox", set("id", "aggregate_id", "event_type", "payload", "created_at", "published_at"));
        expected.put("processed_events", set("event_id", "processed_at"));

        Map<String, Set<String>> actual = actualColumns();
        Set<String> flyway = actual.remove("flyway_schema_history");
        assertThat(flyway).as("flyway_schema_history").isNotNull().contains("version", "script", "success");

        // outbox.traceparent is the plan's M10 additive column: tolerated, not required
        Set<String> outbox = actual.get("outbox");
        if (outbox != null && outbox.contains("traceparent")) {
            Set<String> withoutTrace = new TreeSet<>(outbox);
            withoutTrace.remove("traceparent");
            actual.put("outbox", withoutTrace);
        }
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void columnTypesAndNullability() {
        Map<String, String> actual = new TreeMap<>();
        jdbc.query("select table_name, column_name, data_type, is_nullable, is_identity "
                + "from information_schema.columns where table_schema = 'public'", rs -> {
                    actual.put(rs.getString(1) + "." + rs.getString(2),
                            rs.getString(3) + "|" + rs.getString(4) + "|" + rs.getString(5));
                });
        assertThat(actual).containsEntry("orders.id", "uuid|NO|NO")
                .containsEntry("orders.status", "character varying|NO|NO")
                .containsEntry("orders.total_amount", "numeric|NO|NO")
                .containsEntry("orders.driver_id", "uuid|YES|NO")
                .containsEntry("orders.correlation_id", "uuid|NO|NO")
                .containsEntry("orders.created_at", "timestamp with time zone|NO|NO")
                .containsEntry("orders.version", "bigint|NO|NO")
                .containsEntry("order_status_history.from_status", "character varying|YES|NO")
                .containsEntry("order_status_history.to_status", "character varying|NO|NO")
                .containsEntry("outbox.id", "bigint|NO|YES")
                .containsEntry("outbox.aggregate_id", "uuid|NO|NO")
                .containsEntry("outbox.event_type", "text|NO|NO")
                .containsEntry("outbox.payload", "jsonb|NO|NO")
                .containsEntry("outbox.created_at", "timestamp with time zone|NO|NO")
                .containsEntry("outbox.published_at", "timestamp with time zone|YES|NO")
                .containsEntry("processed_events.event_id", "uuid|NO|NO")
                .containsEntry("processed_events.processed_at", "timestamp with time zone|NO|NO");
    }

    @Test
    void primaryKeys() {
        assertThat(primaryKey("processed_events")).containsExactly("event_id");
        assertThat(primaryKey("outbox")).containsExactly("id");
        assertThat(primaryKey("orders")).containsExactly("id");
        assertThat(primaryKey("order_items")).containsExactly("id");
        assertThat(primaryKey("order_status_history")).containsExactly("id");
    }

    @Test
    void outboxHasPartialIndexOnUnpublishedRows() {
        List<String> defs = jdbc.queryForList(
                "select indexdef from pg_indexes where schemaname = 'public' and tablename = 'outbox'", String.class);
        assertThat(defs).anyMatch(d -> d.toLowerCase().contains("published_at is null"));
    }

    @Test
    void flywayRecordedTheOrderMigrationsAsSuccessful() {
        Set<String> versions = new HashSet<>(jdbc.queryForList(
                "select version from flyway_schema_history where success and version is not null", String.class));
        assertThat(versions).contains("1", "2");
    }

    private List<String> primaryKey(String table) {
        return jdbc.queryForList("select kcu.column_name from information_schema.table_constraints tc "
                + "join information_schema.key_column_usage kcu on tc.constraint_name = kcu.constraint_name "
                + "and tc.table_schema = kcu.table_schema "
                + "where tc.constraint_type = 'PRIMARY KEY' and tc.table_schema = 'public' and tc.table_name = ? "
                + "order by kcu.ordinal_position", String.class, table);
    }
}
