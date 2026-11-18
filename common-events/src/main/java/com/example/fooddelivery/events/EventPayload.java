package com.example.fooddelivery.events;

/** Marker for the 9 event payloads carried inside {@link EventEnvelope#payload()}. */
public sealed interface EventPayload
        permits OrderPlaced, OrderCancelled, OrderAccepted, OrderRejected, OrderReady,
                DriverAssigned, DeliveryFailed, OrderPickedUp, OrderDelivered {
}
