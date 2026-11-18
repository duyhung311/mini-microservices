package com.example.fooddelivery.events;

import java.time.Instant;
import java.util.UUID;

public record OrderReady(UUID restaurantId, Instant readyAt) implements EventPayload {
}
