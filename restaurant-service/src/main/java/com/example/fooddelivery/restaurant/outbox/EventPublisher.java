package com.example.fooddelivery.restaurant.outbox;

import com.example.fooddelivery.events.EventEnvelope;

/** Publishes an event to restaurant.events, keyed by orderId. */
public interface EventPublisher {

    void publish(EventEnvelope envelope);
}
