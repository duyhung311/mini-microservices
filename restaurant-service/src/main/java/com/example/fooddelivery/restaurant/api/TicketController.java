package com.example.fooddelivery.restaurant.api;

import com.example.fooddelivery.restaurant.domain.KitchenTicket;
import com.example.fooddelivery.restaurant.domain.TicketService;
import java.time.Instant;
import java.util.UUID;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TicketController {

    public record TicketView(UUID id, UUID orderId, UUID restaurantId, String status, Instant createdAt) {
    }

    private final TicketService service;

    public TicketController(TicketService service) {
        this.service = service;
    }

    @PostMapping("/tickets/{orderId}/ready")
    public TicketView ready(@PathVariable UUID orderId) {
        return view(service.markReady(orderId));
    }

    @GetMapping("/tickets/{orderId}")
    public TicketView get(@PathVariable UUID orderId) {
        return view(service.find(orderId).orElseThrow(() -> new TicketService.TicketNotFoundException(orderId)));
    }

    private TicketView view(KitchenTicket t) {
        return new TicketView(t.getId(), t.getOrderId(), t.getRestaurantId(), t.getStatus().name(), t.getCreatedAt());
    }
}
