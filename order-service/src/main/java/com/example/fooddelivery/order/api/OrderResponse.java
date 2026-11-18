package com.example.fooddelivery.order.api;

import com.example.fooddelivery.order.domain.Order;
import com.example.fooddelivery.order.domain.OrderStatus;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record OrderResponse(
        UUID orderId,
        UUID customerId,
        UUID restaurantId,
        OrderStatus status,
        BigDecimal totalAmount,
        String deliveryAddress,
        UUID driverId,
        UUID correlationId,
        Instant createdAt,
        Instant updatedAt,
        List<Item> items) {

    public record Item(UUID menuItemId, int quantity, BigDecimal unitPrice) {
    }

    public static OrderResponse from(Order o) {
        return new OrderResponse(o.getId(), o.getCustomerId(), o.getRestaurantId(), o.getStatus(),
                o.getTotalAmount(), o.getDeliveryAddress(), o.getDriverId(), o.getCorrelationId(),
                o.getCreatedAt(), o.getUpdatedAt(),
                o.getItems().stream().map(i -> new Item(i.getMenuItemId(), i.getQuantity(), i.getUnitPrice()))
                        .toList());
    }
}
