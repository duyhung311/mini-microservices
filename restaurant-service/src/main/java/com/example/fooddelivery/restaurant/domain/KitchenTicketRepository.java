package com.example.fooddelivery.restaurant.domain;

import jakarta.persistence.LockModeType;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface KitchenTicketRepository extends JpaRepository<KitchenTicket, UUID> {

    Optional<KitchenTicket> findByOrderId(UUID orderId);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select t from KitchenTicket t where t.orderId = :orderId")
    Optional<KitchenTicket> findForUpdateByOrderId(@Param("orderId") UUID orderId);

    long countByRestaurantIdAndStatus(UUID restaurantId, TicketStatus status);

    /** Used by auto-advance: safe with several instances because locked rows are skipped. */
    @Query(value = "SELECT * FROM kitchen_tickets WHERE status = 'ACCEPTED' AND created_at < :cutoff "
            + "ORDER BY created_at LIMIT :batch FOR UPDATE SKIP LOCKED", nativeQuery = true)
    List<KitchenTicket> lockAcceptedOlderThan(@Param("cutoff") Instant cutoff, @Param("batch") int batch);
}
