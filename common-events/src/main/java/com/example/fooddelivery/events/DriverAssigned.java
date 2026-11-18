package com.example.fooddelivery.events;

import java.util.UUID;

public record DriverAssigned(UUID driverId, String driverName, int etaMinutes) implements EventPayload {
}
