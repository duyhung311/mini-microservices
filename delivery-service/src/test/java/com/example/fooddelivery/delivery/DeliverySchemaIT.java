package com.example.fooddelivery.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.delivery.support.DeliveryIntegrationTestBase;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

/**
 * Criterion 13: delivery_db has exactly the spec section 9 tables and columns plus the documented additive ones.
 * Spec: drivers(id, name, status), deliveries(id, order_id, driver_id, status, pickup_address, dropoff_address,
 * created_at), outbox, processed_events. Plan additions: deliveries.updated_at, nullable driver_id and addresses.
 * Implementer additions: deliveries.correlation_id and deliveries.last_event_id.
 */
class DeliverySchemaIT extends DeliveryIntegrationTestBase {

    private Map<String, Map<String, String>> columns() {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT table_name, column_name, data_type, is_nullable FROM information_schema.columns "
                        + "WHERE table_schema = 'public'");
        Map<String, Map<String, String>> out = new TreeMap<>();
        for (Map<String, Object> r : rows) {
            out.computeIfAbsent((String) r.get("table_name"), k -> new TreeMap<>())
                    .put((String) r.get("column_name"), r.get("data_type") + "/" + r.get("is_nullable"));
        }
        return out;
    }

    @Test
    void exactlyTheExpectedTablesExist() {
        assertThat(columns().keySet()).containsExactlyInAnyOrder(
                "drivers", "deliveries", "outbox", "processed_events", "flyway_schema_history");
    }

    @Test
    void driversTable() {
        assertThat(columns().get("drivers")).containsExactlyInAnyOrderEntriesOf(Map.of(
                "id", "uuid/NO",
                "name", "character varying/NO",
                "status", "character varying/NO"));
    }

    @Test
    void deliveriesTable() {
        assertThat(columns().get("deliveries")).containsExactlyInAnyOrderEntriesOf(Map.ofEntries(
                Map.entry("id", "uuid/NO"),
                Map.entry("order_id", "uuid/NO"),
                Map.entry("driver_id", "uuid/YES"),
                Map.entry("status", "character varying/NO"),
                Map.entry("pickup_address", "character varying/YES"),
                Map.entry("dropoff_address", "character varying/YES"),
                Map.entry("created_at", "timestamp with time zone/NO"),
                Map.entry("updated_at", "timestamp with time zone/NO"),
                Map.entry("correlation_id", "uuid/NO"),
                Map.entry("last_event_id", "uuid/NO")));
    }

    @Test
    void outboxTable() {
        assertThat(columns().get("outbox")).containsExactlyInAnyOrderEntriesOf(Map.of(
                "id", "bigint/NO",
                "aggregate_id", "uuid/NO",
                "event_type", "text/NO",
                "payload", "jsonb/NO",
                "created_at", "timestamp with time zone/NO",
                "published_at", "timestamp with time zone/YES",
                "traceparent", "text/YES"));
        String identity = jdbc.queryForObject("SELECT is_identity FROM information_schema.columns "
                + "WHERE table_schema = 'public' AND table_name = 'outbox' AND column_name = 'id'", String.class);
        assertThat(identity).isEqualTo("YES");
    }

    @Test
    void outboxHasPartialIndexOnUnpublishedRows() {
        List<String> defs = jdbc.queryForList("SELECT indexdef FROM pg_indexes WHERE schemaname = 'public' "
                + "AND tablename = 'outbox'", String.class);
        assertThat(defs).anyMatch(d -> d.toLowerCase().contains("published_at is null"));
    }

    @Test
    void processedEventsTableWithEventIdPrimaryKey() {
        assertThat(columns().get("processed_events")).containsExactlyInAnyOrderEntriesOf(Map.of(
                "event_id", "uuid/NO",
                "processed_at", "timestamp with time zone/NO"));
        List<String> pk = jdbc.queryForList("SELECT kcu.column_name FROM information_schema.table_constraints tc "
                + "JOIN information_schema.key_column_usage kcu ON kcu.constraint_name = tc.constraint_name "
                + "AND kcu.table_schema = tc.table_schema "
                + "WHERE tc.table_schema = 'public' AND tc.table_name = 'processed_events' "
                + "AND tc.constraint_type = 'PRIMARY KEY'", String.class);
        assertThat(pk).containsExactly("event_id");
    }

    @Test
    void flywayHistoryRecordsSuccessfulMigrations() {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT version, success FROM flyway_schema_history WHERE version IS NOT NULL");
        Set<String> versions = new TreeSet<>();
        for (Map<String, Object> r : rows) {
            assertThat(r.get("success")).isEqualTo(Boolean.TRUE);
            versions.add((String) r.get("version"));
        }
        assertThat(versions).contains("1", "2", "3");
    }
}
