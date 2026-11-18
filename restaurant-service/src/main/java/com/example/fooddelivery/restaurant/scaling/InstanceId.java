package com.example.fooddelivery.restaurant.scaling;

import java.util.UUID;

/** Identifies this running instance: app.instance-id (defaults to HOSTNAME) or a random short id. */
public record InstanceId(String value) {

    public static InstanceId resolve(String configured) {
        if (configured != null && !configured.isBlank()) {
            return new InstanceId(configured.trim());
        }
        return new InstanceId(UUID.randomUUID().toString().substring(0, 8));
    }
}
