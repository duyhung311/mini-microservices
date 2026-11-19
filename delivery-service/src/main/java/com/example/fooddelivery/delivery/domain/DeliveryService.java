package com.example.fooddelivery.delivery.domain;

import com.example.fooddelivery.delivery.idempotency.ProcessedEvents;
import com.example.fooddelivery.delivery.outbox.EventPublisher;
import com.example.fooddelivery.events.DeliveryFailed;
import com.example.fooddelivery.events.DeliveryFailureReason;
import com.example.fooddelivery.events.DriverAssigned;
import com.example.fooddelivery.events.Envelopes;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventPayload;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.OrderAccepted;
import com.example.fooddelivery.events.OrderDelivered;
import com.example.fooddelivery.events.OrderPickedUp;
import com.example.fooddelivery.events.OrderPlaced;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * All delivery business logic. Every public method is one transaction. Lock order is always
 * delivery row first, then driver row, so handlers, REST calls and schedulers cannot deadlock.
 */
@Service
public class DeliveryService {

    private static final Logger log = LoggerFactory.getLogger(DeliveryService.class);

    private final DeliveryRepository deliveries;
    private final DriverRepository drivers;
    private final EventPublisher publisher;
    private final ProcessedEvents processed;
    private final int etaMinutes;

    public DeliveryService(DeliveryRepository deliveries, DriverRepository drivers, EventPublisher publisher,
                           ProcessedEvents processed, @Value("${delivery.eta-minutes:20}") int etaMinutes) {
        this.deliveries = deliveries;
        this.drivers = drivers;
        this.publisher = publisher;
        this.processed = processed;
        this.etaMinutes = etaMinutes;
    }

    // ---------------------------------------------------------------- Kafka handlers

    @Transactional
    public void onOrderPlaced(EventEnvelope env, OrderPlaced placed) {
        if (!processed.firstTime(env.eventId())) {
            log.info("duplicate event {} ignored", env.eventId());
            return;
        }
        Delivery d = lockOrCreate(env, placed.deliveryAddress());
        if (d.getDropoffAddress() == null) {
            d.setDropoffAddress(placed.deliveryAddress());
        }
        log.info("OrderPlaced orderId={} delivery={}", env.orderId(), d.getStatus());
    }

    @Transactional
    public void onOrderCancelled(EventEnvelope env) {
        if (!processed.firstTime(env.eventId())) {
            log.info("duplicate event {} ignored", env.eventId());
            return;
        }
        Delivery d = lockOrCreate(env, null);
        switch (d.getStatus()) {
            case ASSIGNED, READY_FOR_PICKUP -> {
                releaseDriver(d);
                d.transition(DeliveryStatus.CANCELLED);
                log.info("OrderCancelled orderId={} delivery cancelled, driver released", env.orderId());
            }
            case PENDING -> {
                d.transition(DeliveryStatus.CANCELLED);
                log.info("OrderCancelled orderId={} tombstone/cancel of pending delivery", env.orderId());
            }
            case PICKED_UP, DELIVERED -> log.warn(
                    "OrderCancelled orderId={} ignored: delivery already {}", env.orderId(), d.getStatus());
            default -> log.info("OrderCancelled orderId={} no-op, delivery is {}", env.orderId(), d.getStatus());
        }
    }

    @Transactional
    public void onOrderAccepted(EventEnvelope env, OrderAccepted accepted) {
        if (!processed.firstTime(env.eventId())) {
            log.info("duplicate event {} ignored", env.eventId());
            return;
        }
        Delivery d = lockOrCreate(env, null);
        if (d.getStatus() != DeliveryStatus.PENDING) {
            log.info("OrderAccepted orderId={} ignored, delivery is {}", env.orderId(), d.getStatus());
            return;
        }
        d.setPickupAddress(accepted.pickupAddress());
        Optional<Driver> driver = drivers.lockFirstAvailable();
        if (driver.isPresent()) {
            Driver dr = driver.get();
            dr.setStatus(DriverStatus.BUSY);
            d.setDriverId(dr.getId());
            d.transition(DeliveryStatus.ASSIGNED);
            emit(d, env, EventTypes.DRIVER_ASSIGNED, new DriverAssigned(dr.getId(), dr.getName(), etaMinutes));
            log.info("OrderAccepted orderId={} driver {} assigned", env.orderId(), dr.getId());
        } else {
            d.transition(DeliveryStatus.FAILED);
            emit(d, env, EventTypes.DELIVERY_FAILED, new DeliveryFailed(DeliveryFailureReason.NO_DRIVER_AVAILABLE));
            log.info("OrderAccepted orderId={} no driver available", env.orderId());
        }
    }

    @Transactional
    public void onOrderReady(EventEnvelope env) {
        if (!processed.firstTime(env.eventId())) {
            log.info("duplicate event {} ignored", env.eventId());
            return;
        }
        Optional<Delivery> found = deliveries.lockByOrderId(env.orderId());
        if (found.isEmpty() || found.get().getStatus() != DeliveryStatus.ASSIGNED) {
            log.info("OrderReady orderId={} ignored, delivery is {}", env.orderId(),
                    found.map(x -> x.getStatus().name()).orElse("absent"));
            return;
        }
        Delivery d = found.get();
        d.transition(DeliveryStatus.READY_FOR_PICKUP);
    }

    // ---------------------------------------------------------------- REST operations

    @Transactional(readOnly = true)
    public List<Driver> listDrivers() {
        return drivers.findAll(Sort.by("id"));
    }

    @Transactional(readOnly = true)
    public Delivery getDelivery(UUID orderId) {
        return deliveries.findByOrderId(orderId)
                .orElseThrow(() -> new NotFoundException("No delivery for order " + orderId));
    }

    @Transactional
    public Driver setDriverStatus(UUID id, DriverStatus target) {
        if (target == null || target == DriverStatus.BUSY) {
            throw new BadRequestException("status must be AVAILABLE or OFFLINE");
        }
        if (!drivers.existsById(id)) {
            throw new NotFoundException("No driver " + id);
        }
        Delivery active = deliveries.lockActiveByDriver(id).orElse(null);
        Driver driver = drivers.lockById(id).orElseThrow(() -> new NotFoundException("No driver " + id));
        if (active == null && driver.getStatus() == DriverStatus.BUSY) {
            // An assignment (onOrderAccepted) may have committed while we waited for the driver lock; its
            // delivery row was invisible to the first query. Look again. This takes the delivery lock while
            // holding the driver lock (reverse of the usual order); onOrderAccepted never waits on a driver row
            // (FOR UPDATE SKIP LOCKED) so it cannot deadlock with us. A remaining cycle needs a handler that
            // locked the just-assigned delivery and then waits for this driver; Postgres aborts one side with a
            // deadlock error, which the REST layer maps to 409 and Kafka handlers retry.
            active = deliveries.lockActiveByDriver(id).orElse(null);
        }
        if (driver.getStatus() != DriverStatus.BUSY) {
            driver.setStatus(target);
            return driver;
        }
        if (target == DriverStatus.AVAILABLE) {
            throw new ConflictException("Driver is BUSY with a delivery; it cannot be set AVAILABLE");
        }
        if (active != null) {
            if (active.getStatus() == DeliveryStatus.PICKED_UP) {
                throw new ConflictException("Driver is carrying a picked-up order; it cannot go OFFLINE");
            }
            active.transition(DeliveryStatus.FAILED);
            emitFirst(active, EventTypes.DELIVERY_FAILED,
                    new DeliveryFailed(DeliveryFailureReason.DRIVER_CANCELLED));
        }
        driver.setStatus(DriverStatus.OFFLINE);
        return driver;
    }

    @Transactional
    public Delivery pickup(UUID orderId) {
        Delivery d = lockExisting(orderId);
        if (d.getStatus() != DeliveryStatus.READY_FOR_PICKUP) {
            throw new ConflictException("Delivery is " + d.getStatus() + ", expected READY_FOR_PICKUP");
        }
        doPickup(d);
        return d;
    }

    @Transactional
    public Delivery deliver(UUID orderId) {
        Delivery d = lockExisting(orderId);
        if (d.getStatus() != DeliveryStatus.PICKED_UP) {
            throw new ConflictException("Delivery is " + d.getStatus() + ", expected PICKED_UP");
        }
        doDeliver(d);
        return d;
    }

    // ---------------------------------------------------------------- auto-advance

    /** Picks up READY_FOR_PICKUP deliveries that have waited since before {@code cutoff}. */
    @Transactional
    public int advancePickups(Instant cutoff) {
        List<Delivery> due = deliveries.lockDue(DeliveryStatus.READY_FOR_PICKUP.name(), cutoff);
        due.forEach(this::doPickup);
        return due.size();
    }

    /** Delivers PICKED_UP deliveries that have waited since before {@code cutoff}. */
    @Transactional
    public int advanceDeliveries(Instant cutoff) {
        List<Delivery> due = deliveries.lockDue(DeliveryStatus.PICKED_UP.name(), cutoff);
        due.forEach(this::doDeliver);
        return due.size();
    }

    // ---------------------------------------------------------------- helpers

    private void doPickup(Delivery d) {
        Instant now = Instant.now();
        d.transition(DeliveryStatus.PICKED_UP);
        emitFirst(d, EventTypes.ORDER_PICKED_UP, new OrderPickedUp(d.getDriverId(), now));
    }

    private void doDeliver(Delivery d) {
        Instant now = Instant.now();
        d.transition(DeliveryStatus.DELIVERED);
        releaseDriver(d);
        emitFirst(d, EventTypes.ORDER_DELIVERED, new OrderDelivered(d.getDriverId(), now));
    }

    private Delivery lockExisting(UUID orderId) {
        return deliveries.lockByOrderId(orderId)
                .orElseThrow(() -> new NotFoundException("No delivery for order " + orderId));
    }

    /** Insert a PENDING row if none exists (race safe), then return it locked. */
    private Delivery lockOrCreate(EventEnvelope env, String dropoff) {
        deliveries.insertPendingIfAbsent(UUID.randomUUID(), env.orderId(), dropoff, env.correlationId());
        return deliveries.lockByOrderId(env.orderId()).orElseThrow();
    }

    private void releaseDriver(Delivery d) {
        if (d.getDriverId() == null) {
            return;
        }
        drivers.lockById(d.getDriverId()).ifPresent(dr -> {
            if (dr.getStatus() == DriverStatus.BUSY) {
                dr.setStatus(DriverStatus.AVAILABLE);
            }
        });
    }

    private void emit(Delivery d, EventEnvelope cause, String type, EventPayload payload) {
        EventEnvelope out = Envelopes.next(cause, type, payload);
        publisher.publish(out);
    }

    /** REST- and scheduler-originated event: causationId null, correlationId of the saga kept. */
    private void emitFirst(Delivery d, String type, EventPayload payload) {
        EventEnvelope out = Envelopes.first(type, d.getOrderId(), d.getCorrelationId(), payload);
        publisher.publish(out);
    }
}
