package com.example.fooddelivery.events;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

public record OrderPlaced(
        UUID customerId,
        UUID restaurantId,
        List<Item> items,
        BigDecimal totalAmount,
        String deliveryAddress) implements EventPayload {

    public record Item(UUID menuItemId, int quantity, BigDecimal unitPrice) {
    }
}
