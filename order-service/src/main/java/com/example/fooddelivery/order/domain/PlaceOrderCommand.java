package com.example.fooddelivery.order.domain;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

public record PlaceOrderCommand(UUID customerId, UUID restaurantId, List<Line> items, String deliveryAddress) {

    public record Line(UUID menuItemId, int quantity, BigDecimal unitPrice) {
    }
}
