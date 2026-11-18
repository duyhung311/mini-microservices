package com.example.fooddelivery.events;

public record OrderCancelled(CancelReason reason) implements EventPayload {
}
