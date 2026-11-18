package com.example.fooddelivery.order.timeout;

import com.example.fooddelivery.events.CancelReason;
import com.example.fooddelivery.events.Envelopes;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.OrderCancelled;
import com.example.fooddelivery.order.domain.Order;
import com.example.fooddelivery.order.domain.OrderInput;
import com.example.fooddelivery.order.domain.OrderRepository;
import com.example.fooddelivery.order.domain.OrderStateMachine;
import com.example.fooddelivery.order.domain.OrderStatus;
import com.example.fooddelivery.order.domain.OrderStatusHistory;
import com.example.fooddelivery.order.domain.OrderStatusHistoryRepository;
import com.example.fooddelivery.order.outbox.EventPublisher;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Transactional steps of the timeout saga. Separate bean so each call is its own transaction. */
@Service
public class OrderTimeoutService {

    private static final Logger log = LoggerFactory.getLogger(OrderTimeoutService.class);

    private final OrderRepository orders;
    private final OrderStatusHistoryRepository history;
    private final EventPublisher publisher;
    private final Clock clock;

    public OrderTimeoutService(OrderRepository orders, OrderStatusHistoryRepository history,
            EventPublisher publisher, Clock clock) {
        this.orders = orders;
        this.history = history;
        this.publisher = publisher;
        this.clock = clock;
    }

    @Transactional(readOnly = true)
    public List<UUID> findCandidates(Instant cutoff, int batch) {
        return orders.findTimedOutPlacedIds(cutoff, batch);
    }

    /** Cancels one order by timeout in one transaction. Returns false if skipped (locked elsewhere, or no longer PLACED). */
    @Transactional
    public boolean timeOut(UUID id, Instant cutoff) {
        Order order = orders.lockTimedOutPlaced(id, cutoff).orElse(null);
        if (order == null) {
            return false;
        }
        OrderStateMachine.Decision d = OrderStateMachine.decide(order.getStatus(), order.getDriverId() != null,
                false, OrderInput.TIMEOUT);
        if (!(d instanceof OrderStateMachine.Apply)) {
            return false;
        }
        OrderStateMachine.Apply apply = (OrderStateMachine.Apply) d;
        EventEnvelope cancelled = Envelopes.first(EventTypes.ORDER_CANCELLED, id, order.getCorrelationId(),
                new OrderCancelled(CancelReason.TIMEOUT));
        Instant now = clock.instant();
        OrderStatus from = order.getStatus();
        order.transitionTo(apply.target(), now);
        orders.save(order);
        history.save(new OrderStatusHistory(id, from, apply.target(), cancelled.eventId(), now));
        publisher.publish(cancelled);
        log.info("Order {} {} -> {} by TIMEOUT (eventId={})", id, from, apply.target(), cancelled.eventId());
        return true;
    }
}
