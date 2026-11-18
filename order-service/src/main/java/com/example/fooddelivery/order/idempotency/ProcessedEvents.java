package com.example.fooddelivery.order.idempotency;

import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/** Records consumed event ids. Must run inside the handler's transaction so a rollback also un-records. */
@Component
public class ProcessedEvents {

    private final JdbcTemplate jdbc;

    public ProcessedEvents(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Returns true if the event was new, false if it was already recorded (duplicate). */
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean markProcessed(UUID eventId) {
        return jdbc.update("INSERT INTO processed_events(event_id) VALUES (?) ON CONFLICT DO NOTHING", eventId) == 1;
    }
}
