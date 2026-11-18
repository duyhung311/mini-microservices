package com.example.fooddelivery.events;

/**
 * The event is valid but its cross-topic cause has not been applied yet.
 * Retryable: the transaction rolls back and the record goes through the retry topics.
 */
public class PrematureEventException extends RuntimeException {
    public PrematureEventException(String message) {
        super(message);
    }

    public PrematureEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
