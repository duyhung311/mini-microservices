package com.example.fooddelivery.events;

import java.time.Instant;
import java.util.UUID;

public record OrderPickedUp(UUID driverId, Instant pickedUpAt) implements EventPayload {
}
