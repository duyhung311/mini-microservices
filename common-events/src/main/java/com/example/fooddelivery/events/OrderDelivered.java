package com.example.fooddelivery.events;

import java.time.Instant;
import java.util.UUID;

public record OrderDelivered(UUID driverId, Instant deliveredAt) implements EventPayload {
}
