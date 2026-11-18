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
    @Column(name = "cause_event_id")
    private UUID causeEventId;

    protected KitchenTicket() {
    }

    public KitchenTicket(UUID orderId, UUID restaurantId, UUID correlationId, UUID causeEventId, Instant createdAt) {
        this.id = UUID.randomUUID();
        this.orderId = orderId;
        this.restaurantId = restaurantId;
        this.status = TicketStatus.ACCEPTED;
        this.createdAt = createdAt;
        this.correlationId = correlationId;
        this.causeEventId = causeEventId;
    }

    public UUID getId() { return id; }
    public UUID getOrderId() { return orderId; }
    public UUID getRestaurantId() { return restaurantId; }
    public TicketStatus getStatus() { return status; }
    public void setStatus(TicketStatus status) { this.status = status; }
    public Instant getCreatedAt() { return createdAt; }
    public UUID getCorrelationId() { return correlationId; }
    public UUID getCauseEventId() { return causeEventId; }
}
