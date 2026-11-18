package com.example.fooddelivery.delivery.api;

import com.example.fooddelivery.delivery.domain.Delivery;
import com.example.fooddelivery.delivery.domain.DeliveryService;
import com.example.fooddelivery.delivery.domain.DeliveryStatus;
import java.time.Instant;
import java.util.UUID;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DeliveryController {

    public record DeliveryResponse(UUID orderId, DeliveryStatus status, UUID driverId, String pickupAddress,
                                   String dropoffAddress, Instant createdAt, Instant updatedAt) {
        static DeliveryResponse of(Delivery d) {
            return new DeliveryResponse(d.getOrderId(), d.getStatus(), d.getDriverId(), d.getPickupAddress(),
                    d.getDropoffAddress(), d.getCreatedAt(), d.getUpdatedAt());
        }
    }

    private final DeliveryService service;

    public DeliveryController(DeliveryService service) {
        this.service = service;
    }

    @GetMapping("/deliveries/{orderId}")
    public DeliveryResponse get(@PathVariable UUID orderId) {
        return DeliveryResponse.of(service.getDelivery(orderId));
    }

    @PostMapping("/deliveries/{orderId}/pickup")
    public DeliveryResponse pickup(@PathVariable UUID orderId) {
        return DeliveryResponse.of(service.pickup(orderId));
    }

    @PostMapping("/deliveries/{orderId}/deliver")
    public DeliveryResponse deliver(@PathVariable UUID orderId) {
        return DeliveryResponse.of(service.deliver(orderId));
    }
}
