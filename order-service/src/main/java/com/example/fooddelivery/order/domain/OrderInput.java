package com.example.fooddelivery.order.domain;

/** Everything that can try to move an order: consumed events, the customer cancel and the timeout job. */
public enum OrderInput {
    ORDER_ACCEPTED,
    ORDER_REJECTED,
    ORDER_READY,
    DRIVER_ASSIGNED,
    DELIVERY_FAILED,
    ORDER_PICKED_UP,
    ORDER_DELIVERED,
    CUSTOMER_CANCEL,
    TIMEOUT
}
