package com.example.fooddelivery.restaurant.support;

import com.example.fooddelivery.events.Envelopes;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.OrderPlaced;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Test fixture: failure injection through a Postgres trigger. The database is shared by every Spring context in
 * the JVM, so unlike a Mockito spy this works no matter which cached context's listener handles the record.
 * A sequence counts the attempts (sequences are not rolled back), so "fail the first N attempts" is exact.
 */
public final class RestaurantFaults {

    private RestaurantFaults() {
    }

    /** An OrderPlaced envelope (REST-style, causationId null) that has not been published yet. */
    public static EventEnvelope orderPlaced(UUID restaurantId, UUID... itemIds) {
        List<OrderPlaced.Item> items = new ArrayList<>();
        for (UUID itemId : itemIds) {
            items.add(new OrderPlaced.Item(itemId, 1, new BigDecimal("12.50")));
        }
        OrderPlaced payload = new OrderPlaced(UUID.randomUUID(), restaurantId, items,
                new BigDecimal("12.50").multiply(BigDecimal.valueOf(itemIds.length)), "5 Customer Ave");
        return Envelopes.first(EventTypes.ORDER_PLACED, UUID.randomUUID(), UUID.randomUUID(), payload);
    }

    /** Installed trigger; close() removes it. */
    public static final class Fault implements AutoCloseable {
        private final JdbcTemplate jdbc;
        private final String name;
        private final String table;

        private Fault(JdbcTemplate jdbc, String name, String table) {
            this.jdbc = jdbc;
            this.name = name;
            this.table = table;
        }

        /** Number of times the trigger has fired for the targeted row. */
        public long attempts() {
            Long n = jdbc.queryForObject("SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM " + name,
                    Long.class);
            return n == null ? 0 : n;
        }

        @Override
        public void close() {
            jdbc.execute("DROP TRIGGER IF EXISTS " + name + " ON " + table);
            jdbc.execute("DROP FUNCTION IF EXISTS " + name + "_fn()");
            jdbc.execute("DROP SEQUENCE IF EXISTS " + name);
        }
    }

    /** Fails the first {@code failFirst} INSERTs into processed_events for this event id (use Long.MAX_VALUE/2 for always). */
    public static Fault failProcessedEventsInsert(JdbcTemplate jdbc, UUID eventId, long failFirst) {
        return install(jdbc, "processed_events", "event_id", eventId, failFirst, false);
    }

    /** Fails every INSERT into outbox for this aggregate immediately. */
    public static Fault failOutboxInsert(JdbcTemplate jdbc, UUID orderId) {
        return install(jdbc, "outbox", "aggregate_id", orderId, Long.MAX_VALUE / 2, false);
    }

    /**
     * Fails the outbox INSERT only at commit time (deferred constraint trigger), i.e. after the business rows have
     * been written in the same transaction.
     */
    public static Fault failOutboxAtCommit(JdbcTemplate jdbc, UUID orderId) {
        return install(jdbc, "outbox", "aggregate_id", orderId, Long.MAX_VALUE / 2, true);
    }

    private static Fault install(JdbcTemplate jdbc, String table, String column, UUID key, long failFirst,
                                 boolean deferred) {
        String name = "t14_" + UUID.randomUUID().toString().replace("-", "");
        jdbc.execute("CREATE SEQUENCE " + name);
        jdbc.execute("CREATE FUNCTION " + name + "_fn() RETURNS trigger LANGUAGE plpgsql AS $body$ BEGIN "
                + "IF NEW." + column + " = '" + key + "'::uuid THEN "
                + "IF nextval('" + name + "') <= " + failFirst + " THEN "
                + "RAISE EXCEPTION 'injected failure'; END IF; END IF; RETURN NEW; END $body$");
        if (deferred) {
            jdbc.execute("CREATE CONSTRAINT TRIGGER " + name + " AFTER INSERT ON " + table
                    + " DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION " + name + "_fn()");
        } else {
            jdbc.execute("CREATE TRIGGER " + name + " BEFORE INSERT ON " + table
                    + " FOR EACH ROW EXECUTE FUNCTION " + name + "_fn()");
        }
        return new Fault(jdbc, name, table);
    }
}
