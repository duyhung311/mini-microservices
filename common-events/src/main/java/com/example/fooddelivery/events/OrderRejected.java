package com.example.fooddelivery.events;

import java.util.UUID;

public record OrderRejected(UUID restaurantId, RejectReason reason) implements EventPayload {
}
