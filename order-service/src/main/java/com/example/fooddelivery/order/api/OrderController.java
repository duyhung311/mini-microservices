package com.example.fooddelivery.order.api;

import com.example.fooddelivery.order.domain.Order;
import com.example.fooddelivery.order.domain.OrderService;
import com.example.fooddelivery.order.domain.OrderStatus;
import com.example.fooddelivery.order.domain.OrderStatusHistory;
import com.example.fooddelivery.order.domain.PlaceOrderCommand;
import jakarta.validation.Valid;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {

    public record PlacedResponse(UUID orderId, OrderStatus status, UUID correlationId) {
    }

    public record HistoryEntry(OrderStatus fromStatus, OrderStatus toStatus, UUID eventId, Instant changedAt) {
    }

    private final OrderService service;

    public OrderController(OrderService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<PlacedResponse> place(@Valid @RequestBody PlaceOrderRequest req) {
        PlaceOrderCommand cmd = new PlaceOrderCommand(req.customerId(), req.restaurantId(),
                req.items().stream()
                        .map(i -> new PlaceOrderCommand.Line(i.menuItemId(), i.quantity(), i.unitPrice()))
                        .toList(),
                req.deliveryAddress());
        Order order = service.place(cmd);
        return ResponseEntity.accepted().location(URI.create("/orders/" + order.getId()))
                .body(new PlacedResponse(order.getId(), order.getStatus(), order.getCorrelationId()));
    }

    @GetMapping("/{id}")
    public OrderResponse get(@PathVariable UUID id) {
        return OrderResponse.from(service.get(id));
    }

    @GetMapping("/{id}/history")
    public List<HistoryEntry> history(@PathVariable UUID id) {
        return service.history(id).stream().map(this::toEntry).toList();
    }

    @PostMapping("/{id}/cancel")
    public OrderResponse cancel(@PathVariable UUID id) {
        return OrderResponse.from(service.cancel(id));
    }

    private HistoryEntry toEntry(OrderStatusHistory h) {
        return new HistoryEntry(h.getFromStatus(), h.getToStatus(), h.getEventId(), h.getChangedAt());
    }
}
