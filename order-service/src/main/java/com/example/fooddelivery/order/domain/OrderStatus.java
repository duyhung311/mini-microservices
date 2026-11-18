package com.example.fooddelivery.order.domain;

public enum OrderStatus {
    PLACED, ACCEPTED, DRIVER_ASSIGNED, READY_FOR_PICKUP, OUT_FOR_DELIVERY, DELIVERED, REJECTED, CANCELLED;

    public boolean isTerminal() {
        return this == DELIVERED || this == REJECTED || this == CANCELLED;
    }
}
