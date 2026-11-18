package com.example.fooddelivery.restaurant.domain;

import com.example.fooddelivery.events.RejectReason;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** Pure decision logic for OrderPlaced. No Spring, no I/O. */
public final class OrderValidator {

    /**
     * @param menuAvailability this restaurant's menu: itemId to available (an item not in the map is foreign or unknown)
     */
    public record RestaurantSnapshot(UUID id, boolean open, int maxConcurrentOrders,
                                     Map<UUID, Boolean> menuAvailability) {
    }

    private OrderValidator() {
    }

    /**
     * Check order: unknown or closed restaurant gives CLOSED; unknown, unavailable or foreign item gives
     * ITEM_UNAVAILABLE; accepted tickets at or above the maximum gives AT_CAPACITY. Empty means accepted.
     *
     * @param restaurant      null when the restaurant is unknown
     * @param acceptedTickets current number of ACCEPTED tickets for the restaurant
     */
    public static Optional<RejectReason> validate(RestaurantSnapshot restaurant, Collection<UUID> itemIds,
                                                  long acceptedTickets) {
        if (restaurant == null || !restaurant.open()) {
            return Optional.of(RejectReason.CLOSED);
        }
        for (UUID itemId : itemIds) {
            Boolean available = restaurant.menuAvailability().get(itemId);
            if (available == null || !available) {
                return Optional.of(RejectReason.ITEM_UNAVAILABLE);
            }
        }
        if (acceptedTickets >= restaurant.maxConcurrentOrders()) {
            return Optional.of(RejectReason.AT_CAPACITY);
        }
        return Optional.empty();
    }
}
