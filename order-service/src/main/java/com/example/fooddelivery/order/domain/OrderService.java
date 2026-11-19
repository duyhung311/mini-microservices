package com.example.fooddelivery.order.domain;

import com.example.fooddelivery.events.CancelReason;
import com.example.fooddelivery.events.DriverAssigned;
import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventPayload;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.Envelopes;
import com.example.fooddelivery.events.OrderCancelled;
import com.example.fooddelivery.events.OrderPlaced;
import com.example.fooddelivery.events.PrematureEventException;
import com.example.fooddelivery.events.tracing.TraceLinks;
import com.example.fooddelivery.order.idempotency.ProcessedEvents;
import com.example.fooddelivery.order.outbox.EventPublisher;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import io.micrometer.tracing.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Order use cases. Every public method is one transaction: state change, history row and event publication. */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orders;
    private final OrderStatusHistoryRepository history;
    private final EventPublisher publisher;
    private final ProcessedEvents processedEvents;
    private final ObjectProvider<Tracer> tracer;

    public OrderService(OrderRepository orders, OrderStatusHistoryRepository history, EventPublisher publisher,
            ProcessedEvents processedEvents, ObjectProvider<Tracer> tracer) {
        this.tracer = tracer;
        this.orders = orders;
        this.history = history;
        this.publisher = publisher;
        this.processedEvents = processedEvents;
    }

    @Transactional
    public Order place(PlaceOrderCommand cmd) {
        Instant now = Instant.now();
        UUID orderId = UUID.randomUUID();
        // M10: the saga trace id is the correlation id (random UUID only when no trace is active)
        UUID correlationId = TraceLinks.currentCorrelationIdOrRandom(tracer.getIfAvailable());
        TraceLinks.tagCurrentObservation(correlationId);
        BigDecimal total = BigDecimal.ZERO;
        for (PlaceOrderCommand.Line l : cmd.items()) {
            total = total.add(l.unitPrice().multiply(BigDecimal.valueOf(l.quantity())));
        }
        Order order = new Order(orderId, cmd.customerId(), cmd.restaurantId(), total, cmd.deliveryAddress(),
                correlationId, now);
        List<OrderPlaced.Item> eventItems = new ArrayList<>();
        for (PlaceOrderCommand.Line l : cmd.items()) {
            order.addItem(new OrderItem(l.menuItemId(), l.quantity(), l.unitPrice()));
            eventItems.add(new OrderPlaced.Item(l.menuItemId(), l.quantity(), l.unitPrice()));
        }
        orders.save(order);
        EventEnvelope env = Envelopes.first(EventTypes.ORDER_PLACED, orderId, correlationId,
                new OrderPlaced(cmd.customerId(), cmd.restaurantId(), eventItems, total, cmd.deliveryAddress()));
        history.save(new OrderStatusHistory(orderId, null, OrderStatus.PLACED, env.eventId(), now));
        publisher.publish(env);
        return order;
    }

    @Transactional(readOnly = true)
    public Order get(UUID id) {
        return orders.findById(id).orElseThrow(() -> new OrderNotFoundException(id));
    }

    @Transactional(readOnly = true)
    public List<OrderStatusHistory> history(UUID id) {
        if (!orders.existsById(id)) {
            throw new OrderNotFoundException(id);
        }
        return history.findByOrderIdOrderByIdAsc(id);
    }

    /** Customer cancel. 404 if unknown, {@link OrderStateConflictException} (409) if not cancellable. */
    @Transactional
    public Order cancel(UUID id) {
        // Row lock (not just @Version): a concurrent applyEvent or timeout makes cancel wait and then decide on the
        // committed state (200 or 409) instead of failing the save with an optimistic-lock error (HTTP 500).
        Order order = orders.findByIdForUpdate(id).orElseThrow(() -> new OrderNotFoundException(id));
        OrderStateMachine.Decision d = OrderStateMachine.decide(order.getStatus(), order.getDriverId() != null,
                false, OrderInput.CUSTOMER_CANCEL);
        if (!(d instanceof OrderStateMachine.Apply)) {
            throw new OrderStateConflictException(id, order.getStatus());
        }
        TraceLinks.tagCurrentObservation(order.getCorrelationId());
        OrderStateMachine.Apply apply = (OrderStateMachine.Apply) d;
        EventEnvelope cancelled = Envelopes.first(EventTypes.ORDER_CANCELLED, id, order.getCorrelationId(),
                new OrderCancelled(CancelReason.CUSTOMER_REQUEST));
        applyTransition(order, apply, cancelled.eventId());
        publisher.publish(cancelled);
        return order;
    }

    /**
     * Applies one consumed event. APPLY updates the order and appends history, STALE logs and returns (recorded as processed),
     * PREMATURE throws so the record is retried. Unknown orders and unhandled types are skipped.
     */
    @Transactional
    public void applyEvent(EventEnvelope env) {
        OrderInput input = inputFor(env.eventType());
        if (input == null) {
            log.debug("Skipping unhandled eventType={} eventId={}", env.eventType(), env.eventId());
            return;
        }
        if (!processedEvents.markProcessed(env.eventId())) {
            log.info("duplicate eventType={} eventId={}, skipping", env.eventType(), env.eventId());
            return;
        }
        Order order = orders.findById(env.orderId()).orElse(null);
        if (order == null) {
            log.warn("Unknown order {} for eventType={} eventId={}, skipping", env.orderId(), env.eventType(),
                    env.eventId());
            return;
        }
        boolean readySeen = history.existsByOrderIdAndToStatus(order.getId(), OrderStatus.READY_FOR_PICKUP);
        OrderStateMachine.Decision d = OrderStateMachine.decide(order.getStatus(), order.getDriverId() != null,
                readySeen, input);
        if (d instanceof OrderStateMachine.Stale) {
            log.info("STALE eventType={} eventId={} orderId={} status={}, ignored", env.eventType(), env.eventId(),
                    order.getId(), order.getStatus());
            return;
        }
        if (d instanceof OrderStateMachine.Premature) {
            throw new PrematureEventException("Premature " + env.eventType() + " for order " + order.getId()
                    + " in state " + order.getStatus());
        }
        OrderStateMachine.Apply apply = (OrderStateMachine.Apply) d;
        if (apply.effects().contains(OrderStateMachine.Effect.SET_DRIVER)) {
            EventPayload payload = EventCodec.payloadOf(env);
            order.assignDriver(((DriverAssigned) payload).driverId());
        }
        applyTransition(order, apply, env.eventId());
        if (apply.effects().contains(OrderStateMachine.Effect.EMIT_CANCELLED_NO_DRIVER)) {
            publisher.publish(Envelopes.next(env, EventTypes.ORDER_CANCELLED,
                    new OrderCancelled(CancelReason.NO_DRIVER)));
        }
    }

    private void applyTransition(Order order, OrderStateMachine.Apply apply, UUID eventId) {
        Instant now = Instant.now();
        OrderStatus from = order.getStatus();
        order.transitionTo(apply.target(), now);
        orders.save(order);
        history.save(new OrderStatusHistory(order.getId(), from, apply.target(), eventId, now));
        log.info("Order {} {} -> {} (eventId={})", order.getId(), from, apply.target(), eventId);
    }

    static OrderInput inputFor(String eventType) {
        switch (eventType) {
            case EventTypes.ORDER_ACCEPTED:
                return OrderInput.ORDER_ACCEPTED;
            case EventTypes.ORDER_REJECTED:
                return OrderInput.ORDER_REJECTED;
            case EventTypes.ORDER_READY:
                return OrderInput.ORDER_READY;
            case EventTypes.DRIVER_ASSIGNED:
                return OrderInput.DRIVER_ASSIGNED;
            case EventTypes.DELIVERY_FAILED:
                return OrderInput.DELIVERY_FAILED;
            case EventTypes.ORDER_PICKED_UP:
                return OrderInput.ORDER_PICKED_UP;
            case EventTypes.ORDER_DELIVERED:
                return OrderInput.ORDER_DELIVERED;
            default:
                return null;
        }
    }
}
