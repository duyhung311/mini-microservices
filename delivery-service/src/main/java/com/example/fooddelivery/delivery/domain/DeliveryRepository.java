package com.example.fooddelivery.delivery.domain;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface DeliveryRepository extends JpaRepository<Delivery, UUID> {

    Optional<Delivery> findByOrderId(UUID orderId);

    @Modifying
    @Query(value = "INSERT INTO deliveries (id, order_id, status, dropoff_address, correlation_id, last_event_id,"
            + " created_at, updated_at) VALUES (:id, :orderId, 'PENDING', :dropoff, :correlationId, :eventId,"
            + " now(), now()) ON CONFLICT (order_id) DO NOTHING", nativeQuery = true)
    int insertPendingIfAbsent(@Param("id") UUID id, @Param("orderId") UUID orderId,
                              @Param("dropoff") String dropoff, @Param("correlationId") UUID correlationId,
                              @Param("eventId") UUID eventId);

    @Query(value = "SELECT * FROM deliveries WHERE order_id = :orderId FOR UPDATE", nativeQuery = true)
    Optional<Delivery> lockByOrderId(@Param("orderId") UUID orderId);

    @Query(value = "SELECT * FROM deliveries WHERE driver_id = :driverId"
            + " AND status IN ('ASSIGNED', 'READY_FOR_PICKUP', 'PICKED_UP') ORDER BY created_at LIMIT 1"
            + " FOR UPDATE", nativeQuery = true)
    Optional<Delivery> lockActiveByDriver(@Param("driverId") UUID driverId);

    @Query(value = "SELECT * FROM deliveries WHERE status = :status AND updated_at < :cutoff"
            + " ORDER BY updated_at LIMIT 20 FOR UPDATE SKIP LOCKED", nativeQuery = true)
    List<Delivery> lockDue(@Param("status") String status, @Param("cutoff") Instant cutoff);
}
