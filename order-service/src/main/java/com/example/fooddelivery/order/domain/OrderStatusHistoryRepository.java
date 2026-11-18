package com.example.fooddelivery.order.domain;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderStatusHistoryRepository extends JpaRepository<OrderStatusHistory, Long> {

    List<OrderStatusHistory> findByOrderIdOrderByIdAsc(UUID orderId);

    boolean existsByOrderIdAndToStatus(UUID orderId, OrderStatus toStatus);
}
