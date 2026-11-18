package com.example.fooddelivery.restaurant.simulation;

import com.example.fooddelivery.restaurant.domain.TicketService;
import java.time.Duration;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Moves ACCEPTED tickets older than simulation.prep-delay to READY. Only exists when simulation.auto-advance=true. */
@Component
@ConditionalOnProperty(name = "simulation.auto-advance", havingValue = "true")
public class TicketAutoAdvancer {

    private static final Logger log = LoggerFactory.getLogger(TicketAutoAdvancer.class);
    private static final int BATCH = 50;

    private final TicketService service;
    private final Duration prepDelay;

    public TicketAutoAdvancer(TicketService service, @Value("${simulation.prep-delay:PT10S}") Duration prepDelay) {
        this.service = service;
        this.prepDelay = prepDelay;
    }

    @Scheduled(fixedDelayString = "${simulation.tick:PT1S}")
    public void tick() {
        try {
            int moved = service.advanceOlderThan(Instant.now().minus(prepDelay), BATCH);
            if (moved > 0) {
                log.info("Auto-advanced {} ticket(s) to READY", moved);
            }
        } catch (RuntimeException e) {
            log.warn("Auto-advance tick failed: {}", e.toString());
        }
    }
}
