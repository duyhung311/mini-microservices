package com.example.fooddelivery.order.domain;

import jakarta.persistence.LockModeType;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface OrderRepository extends JpaRepository<Order, UUID> {

    /** Loads the order with a row lock (select ... for update), so a concurrent writer is waited for, not raced. */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select o from Order o where o.id = :id")
    Optional<Order> findByIdForUpdate(@Param("id") UUID id);

    /** Candidate ids of PLACED orders created before the cutoff (no lock; each is re-checked under lock). */
    @Query(value = "select id from orders where status = 'PLACED' and created_at < :cutoff "
            + "order by created_at limit :batch", nativeQuery = true)
    List<UUID> findTimedOutPlacedIds(@Param("cutoff") Instant cutoff, @Param("batch") int batch);

    /** Locks the order row, skipping it if another instance holds it; empty if no longer a timed-out PLACED order. */
    @Query(value = "select * from orders where id = :id and status = 'PLACED' and created_at < :cutoff "
            + "for update skip locked", nativeQuery = true)
    Optional<Order> lockTimedOutPlaced(@Param("id") UUID id, @Param("cutoff") Instant cutoff);
}
