package com.example.fooddelivery.order.outbox;

import com.example.fooddelivery.events.EventEnvelope;

/** Publishes an event as part of the caller's transaction. M6 swaps the implementation for the outbox. */
public interface EventPublisher {

    void publish(EventEnvelope envelope);
}
