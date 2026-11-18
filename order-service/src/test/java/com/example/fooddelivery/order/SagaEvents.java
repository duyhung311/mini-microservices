package com.example.fooddelivery.order;

import com.example.fooddelivery.events.DeliveryFailed;
import com.example.fooddelivery.events.DeliveryFailureReason;
import com.example.fooddelivery.events.DriverAssigned;
import com.example.fooddelivery.events.Envelopes;
import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.OrderAccepted;
import com.example.fooddelivery.events.OrderDelivered;
import com.example.fooddelivery.events.OrderPickedUp;
import com.example.fooddelivery.events.OrderPlaced;
import com.example.fooddelivery.events.OrderReady;
import com.example.fooddelivery.events.OrderRejected;
import com.example.fooddelivery.events.RejectReason;
import java.time.Instant;
import java.util.UUID;

/** Builds the downstream events a restaurant or delivery service would publish, chained from OrderPlaced. */
final class SagaEvents {

    private SagaEvents() {
    }

    static EventEnvelope accepted(EventEnvelope placed) {
        OrderPlaced p = (OrderPlaced) EventCodec.payloadOf(placed);
        return Envelopes.next(placed, EventTypes.ORDER_ACCEPTED,
                new OrderAccepted(p.restaurantId(), 15, "1 Kitchen St"));
    }

    static EventEnvelope rejected(EventEnvelope placed, RejectReason reason) {
        OrderPlaced p = (OrderPlaced) EventCodec.payloadOf(placed);
        return Envelopes.next(placed, EventTypes.ORDER_REJECTED, new OrderRejected(p.restaurantId(), reason));
    }

    static EventEnvelope ready(EventEnvelope cause) {
        OrderPlaced p = placedPayloadOrNull(cause);
        UUID restaurantId = p != null ? p.restaurantId() : UUID.randomUUID();
        return Envelopes.next(cause, EventTypes.ORDER_READY, new OrderReady(restaurantId, Instant.now()));
    }

    static EventEnvelope driverAssigned(EventEnvelope cause, UUID driverId) {
        return Envelopes.next(cause, EventTypes.DRIVER_ASSIGNED, new DriverAssigned(driverId, "Driver One", 20));
    }

    static EventEnvelope deliveryFailed(EventEnvelope cause, DeliveryFailureReason reason) {
        return Envelopes.next(cause, EventTypes.DELIVERY_FAILED, new DeliveryFailed(reason));
    }

    static EventEnvelope pickedUp(EventEnvelope cause, UUID driverId) {
        return Envelopes.next(cause, EventTypes.ORDER_PICKED_UP, new OrderPickedUp(driverId, Instant.now()));
    }

    static EventEnvelope delivered(EventEnvelope cause, UUID driverId) {
        return Envelopes.next(cause, EventTypes.ORDER_DELIVERED, new OrderDelivered(driverId, Instant.now()));
    }

    private static OrderPlaced placedPayloadOrNull(EventEnvelope e) {
        return EventTypes.ORDER_PLACED.equals(e.eventType()) ? (OrderPlaced) EventCodec.payloadOf(e) : null;
    }
}
