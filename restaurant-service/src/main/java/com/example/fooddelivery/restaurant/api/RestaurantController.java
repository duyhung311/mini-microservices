package com.example.fooddelivery.restaurant.api;

import com.example.fooddelivery.restaurant.domain.MenuItem;
import com.example.fooddelivery.restaurant.domain.MenuItemRepository;
import com.example.fooddelivery.restaurant.domain.Restaurant;
import com.example.fooddelivery.restaurant.domain.RestaurantRepository;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
public class RestaurantController {

    public record MenuItemView(UUID id, String name, BigDecimal price, boolean available) {
    }

    public record RestaurantView(UUID id, String name, String address, boolean open, int maxConcurrentOrders,
                                 List<MenuItemView> menu) {
    }

    public record PatchRequest(Boolean open, @Min(0) Integer maxConcurrentOrders) {
    }

    private final RestaurantRepository restaurants;
    private final MenuItemRepository menuItems;

    public RestaurantController(RestaurantRepository restaurants, MenuItemRepository menuItems) {
        this.restaurants = restaurants;
        this.menuItems = menuItems;
    }

    @GetMapping("/restaurants")
    @Transactional(readOnly = true)
    public List<RestaurantView> list() {
        return restaurants.findAllByOrderByNameAsc().stream().map(this::view).toList();
    }

    @PatchMapping("/restaurants/{id}")
    @Transactional
    public RestaurantView patch(@PathVariable UUID id, @Valid @RequestBody PatchRequest request) {
        Restaurant restaurant = restaurants.findForUpdate(id).orElseThrow(
                () -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Restaurant " + id + " not found"));
        if (request.open() != null) {
            restaurant.setOpen(request.open());
        }
        if (request.maxConcurrentOrders() != null) {
            restaurant.setMaxConcurrentOrders(request.maxConcurrentOrders());
        }
        return view(restaurant);
    }

    private RestaurantView view(Restaurant r) {
        List<MenuItemView> menu = menuItems.findByRestaurantIdOrderByName(r.getId()).stream()
                .map(this::view).toList();
        return new RestaurantView(r.getId(), r.getName(), r.getAddress(), r.isOpen(), r.getMaxConcurrentOrders(),
                menu);
    }

    private MenuItemView view(MenuItem m) {
        return new MenuItemView(m.getId(), m.getName(), m.getPrice(), m.isAvailable());
    }
}
