package com.example.fooddelivery.order.domain;

import java.util.UUID;

public class OrderStateConflictException extends RuntimeException {
    public OrderStateConflictException(UUID id, OrderStatus status) {
        super("Order " + id + " cannot be cancelled in state " + status);
    }
}
