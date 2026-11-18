package com.example.fooddelivery.restaurant.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.UUID;

@Entity
@Table(name = "restaurants")
public class Restaurant {

    @Id
    private UUID id;
    private String name;
    private String address;
    @Column(name = "is_open")
    private boolean open;
    @Column(name = "max_concurrent_orders")
    private int maxConcurrentOrders;

    protected Restaurant() {
    }

    public UUID getId() { return id; }
    public String getName() { return name; }
    public String getAddress() { return address; }
    public boolean isOpen() { return open; }
    public void setOpen(boolean open) { this.open = open; }
    public int getMaxConcurrentOrders() { return maxConcurrentOrders; }
    public void setMaxConcurrentOrders(int maxConcurrentOrders) { this.maxConcurrentOrders = maxConcurrentOrders; }
}
