package com.example.fooddelivery.order.timeout;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Cancels PLACED orders older than placed-max-age (restaurant never answered). Safe with several instances. */
@Component
@ConditionalOnProperty(name = "orders.timeout.enabled", havingValue = "true", matchIfMissing = true)
public class OrderTimeoutJob {

    static final int BATCH_SIZE = 50;
    private static final Logger log = LoggerFactory.getLogger(OrderTimeoutJob.class);

    private final OrderTimeoutService service;
    private final Clock clock;
    private final Duration maxAge;

    public OrderTimeoutJob(OrderTimeoutService service, Clock clock,
            @Value("${orders.timeout.placed-max-age:PT5M}") Duration maxAge) {
        this.service = service;
        this.clock = clock;
        this.maxAge = maxAge;
    }

    @Scheduled(fixedDelayString = "${orders.timeout.scan-interval:PT15S}")
    public void scan() {
        try {
            runOnce();
        } catch (RuntimeException e) {
            log.warn("Order timeout scan failed: {}", e.toString());
        }
    }

    /** One full scan; returns the number of orders cancelled. Public so tests can call it directly. */
    public int runOnce() {
        Instant cutoff = clock.instant().minus(maxAge);
        int total = 0;
        while (true) {
            List<UUID> ids = service.findCandidates(cutoff, BATCH_SIZE);
            int done = 0;
            for (UUID id : ids) {
                if (service.timeOut(id, cutoff)) {
                    done++;
                }
            }
            total += done;
            if (ids.size() < BATCH_SIZE || done == 0) {
                return total;
            }
        }
    }
}
