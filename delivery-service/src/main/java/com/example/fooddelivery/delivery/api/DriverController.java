package com.example.fooddelivery.delivery.api;

import com.example.fooddelivery.delivery.domain.DeliveryService;
import com.example.fooddelivery.delivery.domain.Driver;
import com.example.fooddelivery.delivery.domain.DriverStatus;
import java.util.List;
import java.util.UUID;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DriverController {

    public record DriverResponse(UUID id, String name, DriverStatus status) {
        static DriverResponse of(Driver d) {
            return new DriverResponse(d.getId(), d.getName(), d.getStatus());
        }
    }

    /** {@code status} is parsed as an enum; BUSY and missing values are rejected by the service with 400. */
    public record PatchDriverRequest(DriverStatus status) {
    }

    private final DeliveryService service;

    public DriverController(DeliveryService service) {
        this.service = service;
    }

    @GetMapping("/drivers")
    public List<DriverResponse> list() {
        return service.listDrivers().stream().map(DriverResponse::of).toList();
    }

    @PatchMapping("/drivers/{id}")
    public DriverResponse patch(@PathVariable UUID id, @RequestBody PatchDriverRequest request) {
        return DriverResponse.of(service.setDriverStatus(id, request.status()));
    }
}
