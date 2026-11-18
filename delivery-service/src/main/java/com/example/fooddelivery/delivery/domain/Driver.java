package com.example.fooddelivery.delivery.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.UUID;

@Entity
@Table(name = "drivers")
public class Driver {

    @Id
    private UUID id;

    private String name;

    @Enumerated(EnumType.STRING)
    private DriverStatus status;

    protected Driver() {
    }

    public UUID getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public DriverStatus getStatus() {
        return status;
    }

    public void setStatus(DriverStatus status) {
        this.status = status;
    }
}
