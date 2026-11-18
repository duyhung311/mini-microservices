package com.example.fooddelivery.events;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class EventCodecTest {

    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    /** Numbers compare by value (12.50 equals 12.5); everything else structurally. */
    private static final Comparator<JsonNode> NUMERIC_AWARE = (a, b) -> {
        if (a.isNumber() && b.isNumber()) {
            return a.decimalValue().compareTo(b.decimalValue());
        }
        return a.equals(b) ? 0 : 1;
    };

    private static final Set<String> ENVELOPE_KEYS = Set.of(
            "eventId", "eventType", "eventVersion", "occurredAt", "orderId", "correlationId",
            "causationId", "payload");

    private static final UUID ORDER_ID = UUID.fromString("bbbbbbbb-0000-4000-8000-000000000002");
    private static final UUID CORRELATION_ID = UUID.fromString("cccccccc-0000-4000-8000-000000000003");

    /** eventType and the exact payload field names from spec section 6. */
    static Stream<Arguments> eventTypes() {
        return Stream.of(
                Arguments.of("OrderPlaced",
                        Set.of("customerId", "restaurantId", "items", "totalAmount", "deliveryAddress")),
                Arguments.of("OrderCancelled", Set.of("reason")),
                Arguments.of("OrderAccepted", Set.of("restaurantId", "estimatedPrepMinutes", "pickupAddress")),
                Arguments.of("OrderRejected", Set.of("restaurantId", "reason")),
                Arguments.of("OrderReady", Set.of("restaurantId", "readyAt")),
                Arguments.of("DriverAssigned", Set.of("driverId", "driverName", "etaMinutes")),
                Arguments.of("DeliveryFailed", Set.of("reason")),
                Arguments.of("OrderPickedUp", Set.of("driverId", "pickedUpAt")),
                Arguments.of("OrderDelivered", Set.of("driverId", "deliveredAt")));
    }

    private static String sample(String type) throws IOException {
        try (InputStream in = EventCodecTest.class.getResourceAsStream("/samples/" + type + ".json")) {
            assertThat(in).as("sample file samples/%s.json", type).isNotNull();
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static JsonNode tree(String json) throws IOException {
        return JSON.readTree(json);
    }

    private static ObjectNode sampleTree(String type) throws IOException {
        return (ObjectNode) tree(sample(type));
    }

    // ---- golden samples ----

    @ParameterizedTest(name = "{0}")
    @MethodSource("eventTypes")
    void goldenSampleDecodesAndRoundTrips(String type, Set<String> payloadFields) throws IOException {
        String original = sample(type);

        EventEnvelope decoded = EventCodec.fromJson(original);

        assertThat(decoded.eventType()).isEqualTo(type);
        assertThat(decoded.eventVersion()).isEqualTo(1);
        assertThat(decoded.occurredAt()).isEqualTo(Instant.parse("2026-11-17T12:00:00Z"));
        assertThat(decoded.orderId()).isEqualTo(ORDER_ID);
        assertThat(decoded.correlationId()).isEqualTo(CORRELATION_ID);
        assertThat(EventCodec.payloadOf(decoded)).isNotNull();

        String reencoded = EventCodec.toJson(decoded);
        assertThat(tree(reencoded).equals(NUMERIC_AWARE, tree(original)))
                .as("re-encoded JSON equals golden sample%nactual:   %s%nexpected: %s", reencoded, original)
                .isTrue();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("eventTypes")
    void payloadHasExactlyTheSpecFieldNames(String type, Set<String> payloadFields) throws IOException {
        EventEnvelope decoded = EventCodec.fromJson(sample(type));

        JsonNode payload = tree(EventCodec.toJson(decoded)).get("payload");

        Set<String> actual = new TreeSet<>();
        payload.fieldNames().forEachRemaining(actual::add);
        assertThat(actual).isEqualTo(new TreeSet<>(payloadFields));
    }

    @Test
    void goldenSamplePayloadsDecodeToTheMatchingTypes() throws IOException {
        assertThat(EventCodec.payloadOf(EventCodec.fromJson(sample("OrderPlaced")))).isInstanceOf(OrderPlaced.class);
        assertThat(EventCodec.payloadOf(EventCodec.fromJson(sample("OrderCancelled")))).isInstanceOf(OrderCancelled.class);
        assertThat(EventCodec.payloadOf(EventCodec.fromJson(sample("OrderAccepted")))).isInstanceOf(OrderAccepted.class);
        assertThat(EventCodec.payloadOf(EventCodec.fromJson(sample("OrderRejected")))).isInstanceOf(OrderRejected.class);
        assertThat(EventCodec.payloadOf(EventCodec.fromJson(sample("OrderReady")))).isInstanceOf(OrderReady.class);
        assertThat(EventCodec.payloadOf(EventCodec.fromJson(sample("DriverAssigned")))).isInstanceOf(DriverAssigned.class);
        assertThat(EventCodec.payloadOf(EventCodec.fromJson(sample("DeliveryFailed")))).isInstanceOf(DeliveryFailed.class);
        assertThat(EventCodec.payloadOf(EventCodec.fromJson(sample("OrderPickedUp")))).isInstanceOf(OrderPickedUp.class);
        assertThat(EventCodec.payloadOf(EventCodec.fromJson(sample("OrderDelivered")))).isInstanceOf(OrderDelivered.class);
    }

    @Test
    void orderPlacedItemsHaveSpecFieldNames() throws IOException {
        JsonNode payload = tree(EventCodec.toJson(EventCodec.fromJson(sample("OrderPlaced")))).get("payload");

        JsonNode items = payload.get("items");
        assertThat(items.isArray()).isTrue();
        assertThat(items.size()).isEqualTo(1);
        Set<String> itemFields = new TreeSet<>();
        items.get(0).fieldNames().forEachRemaining(itemFields::add);
        assertThat(itemFields).containsExactlyInAnyOrder("menuItemId", "quantity", "unitPrice");
    }

    // ---- envelope shape ----

    @ParameterizedTest(name = "{0}")
    @MethodSource("eventTypes")
    void encodedEnvelopeHasExactlyTheSpecKeys(String type, Set<String> payloadFields) throws IOException {
        JsonNode encoded = tree(EventCodec.toJson(EventCodec.fromJson(sample(type))));

        Set<String> keys = new TreeSet<>();
        encoded.fieldNames().forEachRemaining(keys::add);
        assertThat(keys).isEqualTo(new TreeSet<>(ENVELOPE_KEYS));
    }

    @Test
    void eventVersionIsNumberOne() throws IOException {
        JsonNode encoded = tree(EventCodec.toJson(EventCodec.fromJson(sample("OrderCancelled"))));

        assertThat(encoded.get("eventVersion").isIntegralNumber()).isTrue();
        assertThat(encoded.get("eventVersion").intValue()).isEqualTo(1);
    }

    @Test
    void occurredAtIsIso8601UtcString() throws IOException {
        JsonNode encoded = tree(EventCodec.toJson(EventCodec.fromJson(sample("OrderCancelled"))));

        JsonNode occurredAt = encoded.get("occurredAt");
        assertThat(occurredAt.isTextual()).as("occurredAt must be a string, not a number").isTrue();
        assertThat(occurredAt.asText()).isEqualTo("2026-11-17T12:00:00Z");
    }

    @Test
    void occurredAtOfGeneratedEnvelopeIsIsoUtcString() throws IOException {
        EventEnvelope env = Envelopes.first("OrderCancelled", ORDER_ID, CORRELATION_ID,
                new OrderCancelled(CancelReason.TIMEOUT));

        JsonNode occurredAt = tree(EventCodec.toJson(env)).get("occurredAt");

        assertThat(occurredAt.isTextual()).isTrue();
        assertThat(occurredAt.asText()).endsWith("Z");
        assertThat(Instant.parse(occurredAt.asText())).isEqualTo(env.occurredAt());
    }

    @Test
    void nullCausationIdIsPresentAsJsonNull() throws IOException {
        EventEnvelope decoded = EventCodec.fromJson(sample("OrderPlaced"));
        assertThat(decoded.causationId()).isNull();

        JsonNode encoded = tree(EventCodec.toJson(decoded));

        assertThat(encoded.has("causationId")).as("causationId key must be present").isTrue();
        assertThat(encoded.get("causationId").isNull()).isTrue();
    }

    @Test
    void absentCausationIdKeyDecodesAsNull() throws IOException {
        ObjectNode n = sampleTree("OrderPlaced");
        n.remove("causationId");

        EventEnvelope decoded = EventCodec.fromJson(n.toString());

        assertThat(decoded.causationId()).isNull();
    }

    @Test
    void nonNullCausationIdRoundTrips() throws IOException {
        EventEnvelope decoded = EventCodec.fromJson(sample("OrderCancelled"));

        assertThat(decoded.causationId()).isEqualTo(UUID.fromString("dddddddd-0000-4000-8000-000000000004"));
        assertThat(tree(EventCodec.toJson(decoded)).get("causationId").asText())
                .isEqualTo("dddddddd-0000-4000-8000-000000000004");
    }

    // ---- amounts ----

    @Test
    void bigDecimalAmountsKeepTheirValue() throws IOException {
        OrderPlaced placed = (OrderPlaced) EventCodec.payloadOf(EventCodec.fromJson(sample("OrderPlaced")));

        assertThat(placed.totalAmount()).isEqualByComparingTo(new BigDecimal("25.00"));
        assertThat(placed.items().get(0).unitPrice()).isEqualByComparingTo(new BigDecimal("12.5"));
        assertThat(placed.items().get(0).unitPrice().compareTo(new BigDecimal("12.50"))).isZero();
        assertThat(placed.items().get(0).quantity()).isEqualTo(2);
    }

    @Test
    void amountsSurviveEncodeDecodeCycle() throws IOException {
        EventEnvelope original = EventCodec.fromJson(sample("OrderPlaced"));

        EventEnvelope again = EventCodec.fromJson(EventCodec.toJson(original));
        OrderPlaced placed = (OrderPlaced) EventCodec.payloadOf(again);

        assertThat(placed.totalAmount()).isEqualByComparingTo("25");
        assertThat(placed.items().get(0).unitPrice()).isEqualByComparingTo("12.5");
    }

    // ---- enums ----

    @ParameterizedTest
    @ValueSource(strings = {"CUSTOMER_REQUEST", "RESTAURANT_REJECTED", "NO_DRIVER", "TIMEOUT"})
    void orderCancelledReasonValues(String reason) throws IOException {
        ObjectNode n = sampleTree("OrderCancelled");
        ((ObjectNode) n.get("payload")).put("reason", reason);

        OrderCancelled payload = (OrderCancelled) EventCodec.payloadOf(EventCodec.fromJson(n.toString()));

        assertThat(payload.reason().name()).isEqualTo(reason);
        assertThat(tree(EventCodec.toJson(EventCodec.fromJson(n.toString()))).get("payload").get("reason").asText())
                .isEqualTo(reason);
    }

    @Test
    void cancelReasonHasExactlyTheSpecValues() {
        assertThat(names(CancelReason.values()))
                .containsExactlyInAnyOrder("CUSTOMER_REQUEST", "RESTAURANT_REJECTED", "NO_DRIVER", "TIMEOUT");
    }

    @ParameterizedTest
    @ValueSource(strings = {"CLOSED", "ITEM_UNAVAILABLE", "AT_CAPACITY"})
    void orderRejectedReasonValues(String reason) throws IOException {
        ObjectNode n = sampleTree("OrderRejected");
        ((ObjectNode) n.get("payload")).put("reason", reason);

        OrderRejected payload = (OrderRejected) EventCodec.payloadOf(EventCodec.fromJson(n.toString()));

        assertThat(payload.reason().name()).isEqualTo(reason);
    }

    @Test
    void rejectReasonHasExactlyTheSpecValues() {
        assertThat(names(RejectReason.values()))
                .containsExactlyInAnyOrder("CLOSED", "ITEM_UNAVAILABLE", "AT_CAPACITY");
    }

    @ParameterizedTest
    @ValueSource(strings = {"NO_DRIVER_AVAILABLE", "DRIVER_CANCELLED"})
    void deliveryFailedReasonValues(String reason) throws IOException {
        ObjectNode n = sampleTree("DeliveryFailed");
        ((ObjectNode) n.get("payload")).put("reason", reason);

        DeliveryFailed payload = (DeliveryFailed) EventCodec.payloadOf(EventCodec.fromJson(n.toString()));

        assertThat(payload.reason().name()).isEqualTo(reason);
    }

    @Test
    void deliveryFailureReasonHasExactlyTheSpecValues() {
        assertThat(names(DeliveryFailureReason.values()))
                .containsExactlyInAnyOrder("NO_DRIVER_AVAILABLE", "DRIVER_CANCELLED");
    }

    @Test
    void unknownEnumValueIsInvalid() throws IOException {
        ObjectNode n = sampleTree("OrderCancelled");
        ((ObjectNode) n.get("payload")).put("reason", "BECAUSE");

        assertThatThrownBy(() -> EventCodec.fromJson(n.toString())).isInstanceOf(InvalidEventException.class);
    }

    // ---- invalid input ----

    @Test
    void malformedJsonIsInvalid() {
        assertThatThrownBy(() -> EventCodec.fromJson("{not json")).isInstanceOf(InvalidEventException.class);
    }

    @Test
    void nonJsonTextIsInvalid() {
        assertThatThrownBy(() -> EventCodec.fromJson("hello world")).isInstanceOf(InvalidEventException.class);
    }

    @Test
    void emptyStringIsInvalid() {
        assertThatThrownBy(() -> EventCodec.fromJson("")).isInstanceOf(InvalidEventException.class);
    }

    @Test
    void unknownEventTypeIsInvalid() throws IOException {
        ObjectNode n = sampleTree("OrderCancelled");
        n.put("eventType", "OrderTeleported");

        assertThatThrownBy(() -> EventCodec.fromJson(n.toString())).isInstanceOf(InvalidEventException.class);
    }

    @Test
    void payloadThatDoesNotMatchEventTypeIsInvalid() throws IOException {
        // an OrderAccepted payload labelled OrderPlaced
        ObjectNode n = sampleTree("OrderAccepted");
        n.put("eventType", "OrderPlaced");

        assertThatThrownBy(() -> EventCodec.fromJson(n.toString())).isInstanceOf(InvalidEventException.class);
    }

    @Test
    void missingEventIdIsInvalid() throws IOException {
        ObjectNode n = sampleTree("OrderCancelled");
        n.remove("eventId");

        assertThatThrownBy(() -> EventCodec.fromJson(n.toString())).isInstanceOf(InvalidEventException.class);
    }

    @Test
    void missingOrderIdIsInvalid() throws IOException {
        ObjectNode n = sampleTree("OrderCancelled");
        n.remove("orderId");

        assertThatThrownBy(() -> EventCodec.fromJson(n.toString())).isInstanceOf(InvalidEventException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"eventType", "eventVersion", "occurredAt", "correlationId", "payload"})
    void otherMissingEnvelopeFieldsAreInvalid(String field) throws IOException {
        ObjectNode n = sampleTree("OrderCancelled");
        n.remove(field);

        assertThatThrownBy(() -> EventCodec.fromJson(n.toString())).isInstanceOf(InvalidEventException.class);
    }

    @Test
    void nullInputIsInvalid() {
        assertThatThrownBy(() -> EventCodec.fromJson(null)).isInstanceOf(InvalidEventException.class);
    }

    private static List<String> names(Enum<?>[] values) {
        List<String> out = new ArrayList<>();
        for (Enum<?> e : values) {
            out.add(e.name());
        }
        return out;
    }
}
