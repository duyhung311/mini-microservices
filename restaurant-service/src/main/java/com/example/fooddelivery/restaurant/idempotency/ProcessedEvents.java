package com.example.fooddelivery.restaurant.idempotency;

import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/** Idempotency guard: records an event id; call it as the first statement of a transactional handler. */
@Component
public class ProcessedEvents {

    private final JdbcTemplate jdbc;

    public ProcessedEvents(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** True if the event was new (and is now recorded), false if it was already processed. */
    public boolean markProcessed(UUID eventId) {
        return jdbc.update("INSERT INTO processed_events (event_id) VALUES (?) ON CONFLICT DO NOTHING", eventId) == 1;
    }
}
