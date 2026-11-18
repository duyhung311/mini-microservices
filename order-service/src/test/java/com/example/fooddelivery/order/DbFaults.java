package com.example.fooddelivery.order;

import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Test-only fault injection done in the database, so it works whichever consumer thread handles a record:
 * a trigger on processed_events that fails the first N inserts for one eventId (the sequence is non-transactional,
 * so a rolled-back attempt still counts), and a trigger on outbox that fails inserts for one aggregate and event type
 * or for one delivery address.
 */
final class DbFaults {

    private DbFaults() {
    }

    /** Removes the trigger and sequence on close. */
    static final class Fault implements AutoCloseable {
        private final JdbcTemplate jdbc;
        private final String table;
        private final String trigger;
        private final String sequence;

        Fault(JdbcTemplate jdbc, String table, String trigger, String sequence) {
            this.jdbc = jdbc;
            this.table = table;
            this.trigger = trigger;
            this.sequence = sequence;
        }

        @Override
        public void close() {
            jdbc.execute("DROP TRIGGER IF EXISTS " + trigger + " ON " + table);
            if (sequence != null) {
                jdbc.execute("DROP SEQUENCE IF EXISTS " + sequence);
            }
        }
    }

    private static String suffix() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    /** Fails the first {@code failures} inserts of processed_events for the eventId with a retryable DB error. */
    static Fault failProcessedEventInsert(JdbcTemplate jdbc, UUID eventId, long failures) {
        jdbc.execute("CREATE OR REPLACE FUNCTION m67_fail_processed_events() RETURNS trigger AS $$ "
                + "BEGIN "
                + "IF NEW.event_id = TG_ARGV[0]::uuid AND nextval(TG_ARGV[2]::regclass) <= TG_ARGV[1]::bigint THEN "
                + "RAISE EXCEPTION 'm67 injected failure for event %', NEW.event_id; "
                + "END IF; RETURN NEW; END $$ LANGUAGE plpgsql");
        String s = suffix();
        String seq = "m67_seq_" + s;
        String trg = "m67_trg_pe_" + s;
        jdbc.execute("CREATE SEQUENCE " + seq);
        jdbc.execute("CREATE TRIGGER " + trg + " BEFORE INSERT ON processed_events FOR EACH ROW "
                + "EXECUTE FUNCTION m67_fail_processed_events('" + eventId + "', '" + failures + "', '" + seq + "')");
        return new Fault(jdbc, "processed_events", trg, seq);
    }

    /** Fails every outbox insert whose OrderPlaced payload has this deliveryAddress. */
    static Fault failOutboxInsertForAddress(JdbcTemplate jdbc, String address) {
        createOutboxFunction(jdbc);
        String trg = "m67_trg_ob_" + suffix();
        jdbc.execute("CREATE TRIGGER " + trg + " BEFORE INSERT ON outbox FOR EACH ROW "
                + "EXECUTE FUNCTION m67_fail_outbox('address', '" + address + "', '')");
        return new Fault(jdbc, "outbox", trg, null);
    }

    /** Fails every outbox insert for the aggregate and event type. */
    static Fault failOutboxInsertFor(JdbcTemplate jdbc, UUID aggregateId, String eventType) {
        createOutboxFunction(jdbc);
        String trg = "m67_trg_ob_" + suffix();
        jdbc.execute("CREATE TRIGGER " + trg + " BEFORE INSERT ON outbox FOR EACH ROW "
                + "EXECUTE FUNCTION m67_fail_outbox('aggregate', '" + aggregateId + "', '" + eventType + "')");
        return new Fault(jdbc, "outbox", trg, null);
    }

    private static void createOutboxFunction(JdbcTemplate jdbc) {
        jdbc.execute("CREATE OR REPLACE FUNCTION m67_fail_outbox() RETURNS trigger AS $$ "
                + "BEGIN "
                + "IF (TG_ARGV[0] = 'address' AND NEW.payload->'payload'->>'deliveryAddress' = TG_ARGV[1]) "
                + "OR (TG_ARGV[0] = 'aggregate' AND NEW.aggregate_id::text = TG_ARGV[1] "
                + "AND NEW.event_type = TG_ARGV[2]) THEN "
                + "RAISE EXCEPTION 'm67 injected outbox failure'; "
                + "END IF; RETURN NEW; END $$ LANGUAGE plpgsql");
    }
}
