package com.example.fooddelivery.restaurant;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.restaurant.support.RestaurantIntegrationTestBase;
import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Criterion 19, restaurant part: REST status codes and shapes. */
class RestaurantApiIT extends RestaurantIntegrationTestBase {

    private JsonNode restaurant(JsonNode list, UUID id) {
        for (JsonNode r : list) {
            if (id.toString().equals(r.get("id").asText())) {
                return r;
            }
        }
        throw new AssertionError("restaurant " + id + " not in " + list);
    }

    private JsonNode menuItem(JsonNode restaurant, UUID id) {
        for (JsonNode m : restaurant.get("menu")) {
            if (id.toString().equals(m.get("id").asText())) {
                return m;
            }
        }
        throw new AssertionError("menu item " + id + " not in " + restaurant);
    }

    @Test
    void getRestaurantsReturnsBothSeedRestaurantsWithMenus() {
        Resp resp = get("/restaurants");

        assertThat(resp.status()).isEqualTo(200);
        assertThat(resp.body().isArray()).isTrue();

        JsonNode pasta = restaurant(resp.body(), PASTA_PLACE);
        assertThat(pasta.get("name").asText()).isEqualTo("Pasta Place");
        assertThat(pasta.get("address").asText()).isEqualTo("1 Kitchen St");
        assertThat(pasta.get("open").asBoolean()).isTrue();
        assertThat(pasta.get("maxConcurrentOrders").asInt()).isEqualTo(5);
        assertThat(pasta.get("menu")).hasSize(3);

        JsonNode margherita = menuItem(pasta, MARGHERITA);
        assertThat(margherita.get("name").asText()).isEqualTo("Margherita");
        assertThat(margherita.get("price").decimalValue()).isEqualByComparingTo(new BigDecimal("12.50"));
        assertThat(margherita.get("available").asBoolean()).isTrue();

        JsonNode lasagne = menuItem(pasta, LASAGNE);
        assertThat(lasagne.get("name").asText()).isEqualTo("Lasagne");
        assertThat(lasagne.get("price").decimalValue()).isEqualByComparingTo(new BigDecimal("14.00"));
        assertThat(lasagne.get("available").asBoolean()).isTrue();

        JsonNode truffle = menuItem(pasta, TRUFFLE);
        assertThat(truffle.get("name").asText()).isEqualTo("Truffle Special");
        assertThat(truffle.get("price").decimalValue()).isEqualByComparingTo(new BigDecimal("30.00"));
        assertThat(truffle.get("available").asBoolean()).isFalse();

        JsonNode night = restaurant(resp.body(), NIGHT_GRILL);
        assertThat(night.get("name").asText()).isEqualTo("Night Grill");
        assertThat(night.get("open").asBoolean()).isFalse();
        assertThat(night.get("menu").isArray()).isTrue();
    }

    @Test
    void patchOpenReturns200AndPersists() {
        try {
            Resp resp = patchRestaurant(PASTA_PLACE, "{\"open\":false}");

            assertThat(resp.status()).isEqualTo(200);
            assertThat(resp.body().get("open").asBoolean()).isFalse();
            assertThat(resp.body().get("maxConcurrentOrders").asInt()).isEqualTo(5);

            JsonNode listed = restaurant(get("/restaurants").body(), PASTA_PLACE);
            assertThat(listed.get("open").asBoolean()).isFalse();
        } finally {
            patchRestaurant(PASTA_PLACE, "{\"open\":true}");
        }
    }

    @Test
    void patchMaxConcurrentOrdersReturns200AndPersists() {
        try {
            Resp resp = patchRestaurant(PASTA_PLACE, "{\"maxConcurrentOrders\":2}");

            assertThat(resp.status()).isEqualTo(200);
            assertThat(resp.body().get("maxConcurrentOrders").asInt()).isEqualTo(2);
            assertThat(resp.body().get("open").asBoolean()).isTrue();

            JsonNode listed = restaurant(get("/restaurants").body(), PASTA_PLACE);
            assertThat(listed.get("maxConcurrentOrders").asInt()).isEqualTo(2);
        } finally {
            patchRestaurant(PASTA_PLACE, "{\"maxConcurrentOrders\":5}");
        }
    }

    @Test
    void patchMaxZeroIsAllowed() {
        try {
            Resp resp = patchRestaurant(PASTA_PLACE, "{\"maxConcurrentOrders\":0}");

            assertThat(resp.status()).isEqualTo(200);
            assertThat(resp.body().get("maxConcurrentOrders").asInt()).isZero();
        } finally {
            patchRestaurant(PASTA_PLACE, "{\"maxConcurrentOrders\":5}");
        }
    }

    @Test
    void patchBothFieldsAtOnce() {
        try {
            Resp resp = patchRestaurant(PASTA_PLACE, "{\"open\":false,\"maxConcurrentOrders\":3}");

            assertThat(resp.status()).isEqualTo(200);
            assertThat(resp.body().get("open").asBoolean()).isFalse();
            assertThat(resp.body().get("maxConcurrentOrders").asInt()).isEqualTo(3);
        } finally {
            patchRestaurant(PASTA_PLACE, "{\"open\":true,\"maxConcurrentOrders\":5}");
        }
    }

    @Test
    void patchNegativeMaxReturns400ProblemJsonAndChangesNothing() {
        Resp resp = patchRestaurant(PASTA_PLACE, "{\"maxConcurrentOrders\":-1}");

        assertThat(resp.status()).isEqualTo(400);
        assertThat(resp.contentType()).startsWith("application/problem+json");
        assertThat(restaurant(get("/restaurants").body(), PASTA_PLACE).get("maxConcurrentOrders").asInt())
                .isEqualTo(5);
    }

    @Test
    void patchUnknownRestaurantReturns404ProblemJson() {
        Resp resp = patchRestaurant(UUID.randomUUID(), "{\"open\":true}");

        assertThat(resp.status()).isEqualTo(404);
        assertThat(resp.contentType()).startsWith("application/problem+json");
    }

    @Test
    void readyForUnknownOrderReturns404ProblemJson() {
        Resp resp = markReady(UUID.randomUUID());

        assertThat(resp.status()).isEqualTo(404);
        assertThat(resp.contentType()).startsWith("application/problem+json");
    }

    @Test
    void getTicketForUnknownOrderReturns404ProblemJson() {
        Resp resp = get("/tickets/" + UUID.randomUUID());

        assertThat(resp.status()).isEqualTo(404);
        assertThat(resp.contentType()).startsWith("application/problem+json");
    }

    @Test
    void getTicketReturnsShapeForAnExistingTicket() {
        var placed = placeOrder(PASTA_PLACE, MARGHERITA);
        awaitTicketStatus(placed.orderId(), "ACCEPTED");

        Resp resp = get("/tickets/" + placed.orderId());

        assertThat(resp.status()).isEqualTo(200);
        assertThat(resp.body().get("orderId").asText()).isEqualTo(placed.orderId().toString());
        assertThat(resp.body().get("restaurantId").asText()).isEqualTo(PASTA_PLACE.toString());
        assertThat(resp.body().get("status").asText()).isEqualTo("ACCEPTED");
        assertThat(resp.body().get("id").asText()).isNotBlank();
        assertThat(resp.body().get("createdAt").asText()).isNotBlank();
    }
}
