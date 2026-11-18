package com.example.fooddelivery.delivery.domain;

import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface DriverRepository extends JpaRepository<Driver, UUID> {

    @Query(value = "SELECT * FROM drivers WHERE status = 'AVAILABLE' ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED",
            nativeQuery = true)
    Optional<Driver> lockFirstAvailable();

    @Query(value = "SELECT * FROM drivers WHERE id = :id FOR UPDATE", nativeQuery = true)
    Optional<Driver> lockById(@Param("id") UUID id);
}
