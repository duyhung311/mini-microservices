package com.example.fooddelivery.restaurant.domain;

import com.example.fooddelivery.events.Envelopes;
import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventPayload;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.OrderAccepted;
import com.example.fooddelivery.events.OrderPlaced;
import com.example.fooddelivery.events.OrderReady;
import com.example.fooddelivery.events.OrderRejected;
import com.example.fooddelivery.events.RejectReason;
import com.example.fooddelivery.restaurant.idempotency.ProcessedEvents;
import com.example.fooddelivery.restaurant.outbox.EventPublisher;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/** One transactional method per handler / use case. */
@Service
public class TicketService {

    private static final Logger log = LoggerFactory.getLogger(TicketService.class);

    private final RestaurantRepository restaurants;
    private final MenuItemRepository menuItems;
    private final KitchenTicketRepository tickets;
    private final EventPublisher publisher;
    private final ProcessedEvents processedEvents;
    private final MeterRegistry meters;
    private final int prepMinutes;

    public TicketService(RestaurantRepository restaurants, MenuItemRepository menuItems,
                         KitchenTicketRepository tickets, EventPublisher publisher, ProcessedEvents processedEvents,
                         MeterRegistry meters, @Value("${restaurant.prep-minutes:15}") int prepMinutes) {
        this.restaurants = restaurants;
        this.menuItems = menuItems;
        this.tickets = tickets;
        this.publisher = publisher;
        this.processedEvents = processedEvents;
        this.meters = meters;
        this.prepMinutes = prepMinutes;
    }

    @Transactional
    public void handleOrderPlaced(EventEnvelope envelope) {
        if (!processedEvents.markProcessed(envelope.eventId())) {
            log.info("duplicate event {} ({}); skipping", envelope.eventId(), envelope.eventType());
            return;
        }
        OrderPlaced placed = (OrderPlaced) EventCodec.payloadOf(envelope);
        Optional<KitchenTicket> existing = tickets.findByOrderId(envelope.orderId());
        if (existing.isPresent() && existing.get().getStatus() == TicketStatus.CANCELLED) {
            // CANCELLED is terminal: OrderCancelled overtook this OrderPlaced (e.g. it was retried). No decision event.
            log.info("OrderPlaced for order {} which is already CANCELLED; ignoring (eventId={})",
                    envelope.orderId(), envelope.eventId());
            return;
        }
        if (existing.isPresent()) {
            log.warn("OrderPlaced for order {} which already has a ticket; skipping (eventId={})",
                    envelope.orderId(), envelope.eventId());
            return;
        }
        Optional<Restaurant> restaurant = restaurants.findForUpdate(placed.restaurantId());
        OrderValidator.RestaurantSnapshot snapshot = null;
        long accepted = 0;
        if (restaurant.isPresent()) {
            Restaurant r = restaurant.get();
            Map<UUID, Boolean> menu = new HashMap<>();
            for (MenuItem item : menuItems.findByRestaurantIdOrderByName(r.getId())) {
                menu.put(item.getId(), item.isAvailable());
            }
            snapshot = new OrderValidator.RestaurantSnapshot(r.getId(), r.isOpen(), r.getMaxConcurrentOrders(), menu);
            accepted = tickets.countByRestaurantIdAndStatus(r.getId(), TicketStatus.ACCEPTED);
        }
        List<UUID> itemIds = placed.items().stream().map(OrderPlaced.Item::menuItemId).toList();
        Optional<RejectReason> rejection = OrderValidator.validate(snapshot, itemIds, accepted);
        if (rejection.isPresent()) {
            log.info("Rejecting order {}: {}", envelope.orderId(), rejection.get());
            publish(envelope, EventTypes.ORDER_REJECTED, new OrderRejected(placed.restaurantId(), rejection.get()));
            countHandled("rejected");
            return;
        }
        tickets.save(new KitchenTicket(envelope.orderId(), placed.restaurantId(), envelope.correlationId(),
                Instant.now()));
        log.info("Accepted order {}", envelope.orderId());
        publish(envelope, EventTypes.ORDER_ACCEPTED,
                new OrderAccepted(placed.restaurantId(), prepMinutes, restaurant.get().getAddress()));
        countHandled("accepted");
    }

    /** Counts a handled OrderPlaced once, after the transaction commits. */
    private void countHandled(String outcome) {
        Runnable inc = () -> meters.counter("restaurant.orders.handled", "outcome", outcome).increment();
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    inc.run();
                }
            });
        } else {
            inc.run();
        }
    }

    @Transactional
    public void handleOrderCancelled(EventEnvelope envelope) {
        if (!processedEvents.markProcessed(envelope.eventId())) {
            log.info("duplicate event {} ({}); skipping", envelope.eventId(), envelope.eventType());
            return;
        }
        Optional<KitchenTicket> found = tickets.findForUpdateByOrderId(envelope.orderId());
        if (found.isEmpty()) {
            // Tombstone: a later OrderPlaced for this order must not create an ACCEPTED ticket.
            tickets.save(KitchenTicket.tombstone(envelope.orderId(), envelope.correlationId(), Instant.now()));
            log.info("OrderCancelled for order {} without a ticket; recorded a CANCELLED tombstone",
                    envelope.orderId());
            return;
        }
        KitchenTicket ticket = found.get();
        if (ticket.getStatus() == TicketStatus.CANCELLED) {
            return;
        }
        ticket.setStatus(TicketStatus.CANCELLED);
        log.info("Ticket for order {} is now CANCELLED", envelope.orderId());
    }

    @Transactional
    public void handleDeliveryFailed(EventEnvelope envelope) {
        if (!processedEvents.markProcessed(envelope.eventId())) {
            log.info("duplicate event {} ({}); skipping", envelope.eventId(), envelope.eventType());
            return;
        }
        Optional<KitchenTicket> found = tickets.findForUpdateByOrderId(envelope.orderId());
        if (found.isEmpty()) {
            log.info("DeliveryFailed for order {} without a ticket; skipping", envelope.orderId());
            return;
        }
        KitchenTicket ticket = found.get();
        if (ticket.getStatus() == TicketStatus.ACCEPTED || ticket.getStatus() == TicketStatus.READY) {
            ticket.setStatus(TicketStatus.ABANDONED);
            log.info("Ticket for order {} is now ABANDONED", envelope.orderId());
        } else {
            log.info("DeliveryFailed for order {} with ticket {}; no-op", envelope.orderId(), ticket.getStatus());
        }
    }

    /** REST path. Throws on a missing ticket or a ticket that is not ACCEPTED. */
    @Transactional
    public KitchenTicket markReady(UUID orderId) {
        KitchenTicket ticket = tickets.findForUpdateByOrderId(orderId)
                .filter(t -> t.getRestaurantId() != null)
                .orElseThrow(() -> new TicketNotFoundException(orderId));
        if (ticket.getStatus() != TicketStatus.ACCEPTED) {
            throw new TicketConflictException(orderId, ticket.getStatus());
        }
        becomeReady(ticket);
        return ticket;
    }

    /** Scheduler path: moves ACCEPTED tickets created before the cutoff to READY. Returns how many. */
    @Transactional
    public int advanceOlderThan(Instant cutoff, int batch) {
        List<KitchenTicket> due = tickets.lockAcceptedOlderThan(cutoff, batch);
        due.forEach(this::becomeReady);
        return due.size();
    }

    @Transactional(readOnly = true)
    public Optional<KitchenTicket> find(UUID orderId) {
        // tombstones (no restaurant) are not tickets the API exposes
        return tickets.findByOrderId(orderId).filter(t -> t.getRestaurantId() != null);
    }

    private void becomeReady(KitchenTicket ticket) {
        ticket.setStatus(TicketStatus.READY);
        // REST- or scheduler-originated: causationId is null, correlationId is the order's.
        publisher.publish(Envelopes.first(EventTypes.ORDER_READY, ticket.getOrderId(), ticket.getCorrelationId(),
                new OrderReady(ticket.getRestaurantId(), Instant.now())));
        log.info("Ticket for order {} is now READY", ticket.getOrderId());
    }

    private void publish(EventEnvelope cause, String type, EventPayload payload) {
        publisher.publish(Envelopes.next(cause, type, payload));
    }

    public static class TicketNotFoundException extends RuntimeException {
        public TicketNotFoundException(UUID orderId) {
            super("No ticket for order " + orderId);
        }
    }

    public static class TicketConflictException extends RuntimeException {
        public TicketConflictException(UUID orderId, TicketStatus status) {
            super("Ticket for order " + orderId + " is " + status + ", expected ACCEPTED");
        }
    }
}
