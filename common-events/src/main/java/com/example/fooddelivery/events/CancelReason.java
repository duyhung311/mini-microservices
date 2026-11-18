package com.example.fooddelivery.events;

public enum CancelReason {
    CUSTOMER_REQUEST,
    /** Reserved, never emitted in v1 (a rejected order is terminal, no OrderCancelled is published). */
    RESTAURANT_REJECTED,
    NO_DRIVER,
    TIMEOUT
}
