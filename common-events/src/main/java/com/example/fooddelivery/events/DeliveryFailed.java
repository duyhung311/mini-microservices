package com.example.fooddelivery.events;

public record DeliveryFailed(DeliveryFailureReason reason) implements EventPayload {
}
