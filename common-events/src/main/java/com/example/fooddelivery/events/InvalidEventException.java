package com.example.fooddelivery.events;

/** The record cannot be decoded or is malformed. Poison message: never retried. */
public class InvalidEventException extends RuntimeException {
    public InvalidEventException(String message) {
        super(message);
    }

    public InvalidEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
