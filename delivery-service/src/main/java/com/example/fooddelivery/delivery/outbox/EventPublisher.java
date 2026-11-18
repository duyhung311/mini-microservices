package com.example.fooddelivery.delivery.outbox;

import com.example.fooddelivery.events.EventEnvelope;

/** Publishes an envelope to delivery.events. */
public interface EventPublisher {

    void publish(EventEnvelope envelope);
}
