package com.example.fooddelivery.events;

import java.util.UUID;

public record OrderAccepted(UUID restaurantId, int estimatedPrepMinutes, String pickupAddress)
        implements EventPayload {
}
