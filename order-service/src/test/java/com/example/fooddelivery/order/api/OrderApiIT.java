package com.example.fooddelivery.order.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.order.AbstractOrderIT;
import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.http.ResponseEntity;

/** Criterion 19 (order side): every status code listed under REST > Order. */
class OrderApiIT extends AbstractOrderIT {

    @Test
    void postReturns202WithLocationAndPlacedBody() {
        ResponseEntity<JsonNode> r = postRaw("/orders", validOrderJson());

        assertThat(r.getStatusCode().value()).isEqualTo(202);
        JsonNode body = r.getBody();
        UUID orderId = UUID.fromString(body.get("orderId").asText());
        assertThat(body.get("status").asText()).isEqualTo("PLACED");
        assertThat(UUID.fromString(body.get("correlationId").asText())).isNotNull();
        assertThat(r.getHeaders().getLocation()).isNotNull();
        assertThat(r.getHeaders().getLocation().toString()).endsWith("/orders/" + orderId);
    }

    @Test
    void twoPostsGetDifferentOrderIds() {
        JsonNode a = placeOrder();
        JsonNode b = placeOrder();
        assertThat(a.get("orderId").asText()).isNotEqualTo(b.get("orderId").asText());
    }

    static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> invalidBodies() {
        UUID c = UUID.randomUUID();
        UUID r = UUID.randomUUID();
        UUID m = UUID.randomUUID();
        String okItems = "[" + itemJson(m, 1, "10.00") + "]";
        return java.util.stream.Stream.of(
                org.junit.jupiter.params.provider.Arguments.of("empty items", orderJson(c, r, "[]", "1 Main St")),
                org.junit.jupiter.params.provider.Arguments.of("missing items", orderJson(c, r, null, "1 Main St")),
                org.junit.jupiter.params.provider.Arguments.of("quantity 0",
                        orderJson(c, r, "[" + itemJson(m, 0, "10.00") + "]", "1 Main St")),
                org.junit.jupiter.params.provider.Arguments.of("negative quantity",
                        orderJson(c, r, "[" + itemJson(m, -1, "10.00") + "]", "1 Main St")),
                org.junit.jupiter.params.provider.Arguments.of("unitPrice 0",
                        orderJson(c, r, "[" + itemJson(m, 1, "0") + "]", "1 Main St")),
                org.junit.jupiter.params.provider.Arguments.of("unitPrice negative",
                        orderJson(c, r, "[" + itemJson(m, 1, "-3.50") + "]", "1 Main St")),
                org.junit.jupiter.params.provider.Arguments.of("missing customerId",
                        orderJson(null, r, okItems, "1 Main St")),
                org.junit.jupiter.params.provider.Arguments.of("missing restaurantId",
                        orderJson(c, null, okItems, "1 Main St")),
                org.junit.jupiter.params.provider.Arguments.of("missing deliveryAddress",
                        orderJson(c, r, okItems, null)),
                org.junit.jupiter.params.provider.Arguments.of("blank deliveryAddress",
                        orderJson(c, r, okItems, " ")),
                org.junit.jupiter.params.provider.Arguments.of("item without menuItemId",
                        orderJson(c, r, "[{\"quantity\":1,\"unitPrice\":10.00}]", "1 Main St")),
                org.junit.jupiter.params.provider.Arguments.of("empty object", "{}"),
                org.junit.jupiter.params.provider.Arguments.of("malformed json", "{not json"));
    }

    @ParameterizedTest(name = "400 for {0}")
    @MethodSource("invalidBodies")
    void invalidInputReturns400ProblemDetail(String name, String json) {
        ResponseEntity<JsonNode> r = postRaw("/orders", json);

        assertThat(r.getStatusCode().value()).isEqualTo(400);
        assertThat(r.getBody()).isNotNull();
        assertThat(r.getBody().path("status").asInt()).isEqualTo(400);
        assertThat(r.getHeaders().getContentType().toString()).contains("problem+json");
    }

    @Test
    void getReturns200WithOrderResponse() {
        UUID customerId = UUID.randomUUID();
        UUID restaurantId = UUID.randomUUID();
        UUID menuItemId = UUID.randomUUID();
        JsonNode placed = postRaw("/orders",
                orderJson(customerId, restaurantId, "[" + itemJson(menuItemId, 2, "12.50") + "]", "5 Oak Ave"))
                .getBody();
        UUID orderId = UUID.fromString(placed.get("orderId").asText());

        ResponseEntity<JsonNode> r = get("/orders/" + orderId);

        assertThat(r.getStatusCode().value()).isEqualTo(200);
        JsonNode o = r.getBody();
        assertThat(o.get("orderId").asText()).isEqualTo(orderId.toString());
        assertThat(o.get("customerId").asText()).isEqualTo(customerId.toString());
        assertThat(o.get("restaurantId").asText()).isEqualTo(restaurantId.toString());
        assertThat(o.get("status").asText()).isEqualTo("PLACED");
        assertThat(o.get("deliveryAddress").asText()).isEqualTo("5 Oak Ave");
        assertThat(o.get("driverId").isNull()).isTrue();
        assertThat(o.get("correlationId").asText()).isEqualTo(placed.get("correlationId").asText());
        assertThat(o.get("createdAt").isNull()).isFalse();
        assertThat(o.get("updatedAt").isNull()).isFalse();
        assertThat(o.get("items")).hasSize(1);
        assertThat(o.get("items").get(0).get("menuItemId").asText()).isEqualTo(menuItemId.toString());
        assertThat(o.get("items").get(0).get("quantity").asInt()).isEqualTo(2);
        assertThat(o.get("items").get(0).get("unitPrice").decimalValue()).isEqualByComparingTo("12.50");
    }

    @Test
    void serverComputesTotalAmount() {
        UUID restaurantId = UUID.randomUUID();
        String items = "[" + itemJson(UUID.randomUUID(), 2, "12.50") + "]";
        JsonNode placed = postRaw("/orders", orderJson(UUID.randomUUID(), restaurantId, items, "1 Main St")).getBody();

        JsonNode o = get("/orders/" + placed.get("orderId").asText()).getBody();

        assertThat(o.get("totalAmount").decimalValue().compareTo(new BigDecimal("25.00"))).isZero();
    }

    @Test
    void serverComputesTotalAmountAcrossItems() {
        String items = "[" + itemJson(UUID.randomUUID(), 2, "12.50") + "," + itemJson(UUID.randomUUID(), 1, "14.00")
                + "]";
        JsonNode placed = postRaw("/orders", orderJson(UUID.randomUUID(), UUID.randomUUID(), items, "1 Main St"))
                .getBody();

        JsonNode o = get("/orders/" + placed.get("orderId").asText()).getBody();

        assertThat(o.get("totalAmount").decimalValue().compareTo(new BigDecimal("39.00"))).isZero();
    }

    @Test
    void getUnknownOrderReturns404() {
        ResponseEntity<JsonNode> r = get("/orders/" + UUID.randomUUID());
        assertThat(r.getStatusCode().value()).isEqualTo(404);
        assertThat(r.getBody().path("status").asInt()).isEqualTo(404);
    }

    @Test
    void historyContainsInitialNullToPlacedRow() {
        JsonNode placed = placeOrder();

        ResponseEntity<JsonNode> r = get("/orders/" + placed.get("orderId").asText() + "/history");

        assertThat(r.getStatusCode().value()).isEqualTo(200);
        JsonNode h = r.getBody();
        assertThat(h.isArray()).isTrue();
        assertThat(h).hasSize(1);
        assertThat(h.get(0).get("fromStatus").isNull()).isTrue();
        assertThat(h.get(0).get("toStatus").asText()).isEqualTo("PLACED");
        assertThat(h.get(0).get("eventId").isNull()).isFalse();
        assertThat(h.get(0).get("changedAt").isNull()).isFalse();
    }

    @Test
    void historyOfUnknownOrderReturns404() {
        ResponseEntity<JsonNode> r = get("/orders/" + UUID.randomUUID() + "/history");
        assertThat(r.getStatusCode().value()).isEqualTo(404);
    }

    @Test
    void cancelUnknownOrderReturns404() {
        ResponseEntity<JsonNode> r = post("/orders/" + UUID.randomUUID() + "/cancel");
        assertThat(r.getStatusCode().value()).isEqualTo(404);
        assertThat(r.getBody().path("status").asInt()).isEqualTo(404);
    }

    @Test
    void cancelPlacedOrderReturns200WithOrderBodyThenSecondCancelIs409() {
        JsonNode placed = placeOrder();
        String id = placed.get("orderId").asText();

        ResponseEntity<JsonNode> first = post("/orders/" + id + "/cancel");
        assertThat(first.getStatusCode().value()).isEqualTo(200);
        assertThat(first.getBody().get("orderId").asText()).isEqualTo(id);
        assertThat(first.getBody().get("status").asText()).isEqualTo("CANCELLED");

        ResponseEntity<JsonNode> second = post("/orders/" + id + "/cancel");
        assertThat(second.getStatusCode().value()).isEqualTo(409);
        assertThat(second.getBody().path("status").asInt()).isEqualTo(409);
    }
}
