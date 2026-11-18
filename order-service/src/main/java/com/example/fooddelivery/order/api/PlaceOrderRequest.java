package com.example.fooddelivery.order.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

public record PlaceOrderRequest(
        @NotNull UUID customerId,
        @NotNull UUID restaurantId,
        @NotEmpty List<@NotNull @Valid Item> items,
        @NotBlank String deliveryAddress) {

    public record Item(
            @NotNull UUID menuItemId,
            @Min(1) int quantity,
            @NotNull @DecimalMin(value = "0", inclusive = false) BigDecimal unitPrice) {
    }
}
