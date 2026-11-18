package com.example.fooddelivery.delivery.simulation;

import com.example.fooddelivery.delivery.domain.DeliveryService;
import java.time.Duration;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * With simulation.auto-advance=true, moves READY_FOR_PICKUP to PICKED_UP to DELIVERED on timers.
 * Rows are picked with FOR UPDATE SKIP LOCKED in {@link DeliveryService}, so several instances are safe.
 */
@Component
@ConditionalOnProperty(name = "simulation.auto-advance", havingValue = "true")
public class AutoAdvanceScheduler {

    private static final Logger log = LoggerFactory.getLogger(AutoAdvanceScheduler.class);

    private final DeliveryService service;
    private final Duration pickupDelay;
    private final Duration deliveryDelay;

    public AutoAdvanceScheduler(DeliveryService service,
                                @Value("${simulation.pickup-delay:PT5S}") Duration pickupDelay,
                                @Value("${simulation.delivery-delay:PT10S}") Duration deliveryDelay) {
        this.service = service;
        this.pickupDelay = pickupDelay;
        this.deliveryDelay = deliveryDelay;
    }

    @Scheduled(fixedDelayString = "${simulation.tick:PT1S}")
    public void tick() {
        try {
            int picked = service.advancePickups(Instant.now().minus(pickupDelay));
            int delivered = service.advanceDeliveries(Instant.now().minus(deliveryDelay));
            if (picked + delivered > 0) {
                log.info("Auto-advance: pickedUp={} delivered={}", picked, delivered);
            }
        } catch (RuntimeException e) {
            log.error("Auto-advance tick failed", e);
        }
    }
}
