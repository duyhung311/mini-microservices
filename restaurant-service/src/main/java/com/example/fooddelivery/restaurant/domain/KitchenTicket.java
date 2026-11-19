package com.example.fooddelivery.restaurant.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "kitchen_tickets")
public class KitchenTicket {

    @Id
    private UUID id;
    @Column(name = "order_id")
    private UUID orderId;
    @Column(name = "restaurant_id")
    private UUID restaurantId;
    @Enumerated(EnumType.STRING)
    private TicketStatus status;
    @Column(name = "created_at")
    private Instant createdAt;
    @Column(name = "correlation_id")
    private UUID correlationId;

    /** CANCELLED row recorded when OrderCancelled is handled before its OrderPlaced; restaurant is unknown (null). */
    public static KitchenTicket tombstone(UUID orderId, UUID correlationId, Instant createdAt) {
        KitchenTicket ticket = new KitchenTicket(orderId, null, correlationId, createdAt);
        ticket.status = TicketStatus.CANCELLED;
        return ticket;
    }

    protected KitchenTicket() {
    }

    public KitchenTicket(UUID orderId, UUID restaurantId, UUID correlationId, Instant createdAt) {
        this.id = UUID.randomUUID();
        this.orderId = orderId;
        this.restaurantId = restaurantId;
        this.status = TicketStatus.ACCEPTED;
        this.createdAt = createdAt;
        this.correlationId = correlationId;
    }

    public UUID getId() { return id; }
    public UUID getOrderId() { return orderId; }
    public UUID getRestaurantId() { return restaurantId; }
    public TicketStatus getStatus() { return status; }
    public void setStatus(TicketStatus status) { this.status = status; }
    public Instant getCreatedAt() { return createdAt; }
    public UUID getCorrelationId() { return correlationId; }
}
