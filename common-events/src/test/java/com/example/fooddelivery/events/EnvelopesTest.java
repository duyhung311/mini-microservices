package com.example.fooddelivery.events;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class EnvelopesTest {

    private final UUID orderId = UUID.randomUUID();
    private final UUID correlationId = UUID.randomUUID();

    private EventEnvelope firstCancelled() {
        return Envelopes.first("OrderCancelled", orderId, correlationId, new OrderCancelled(CancelReason.TIMEOUT));
    }

    @Test
    void firstSetsOrderCorrelationVersionAndNullCausation() {
        EventEnvelope env = firstCancelled();

        assertThat(env.eventType()).isEqualTo("OrderCancelled");
        assertThat(env.eventVersion()).isEqualTo(1);
        assertThat(env.orderId()).isEqualTo(orderId);
        assertThat(env.correlationId()).isEqualTo(correlationId);
        assertThat(env.causationId()).isNull();
        assertThat(env.eventId()).isNotNull();
        assertThat(env.occurredAt()).isNotNull();
        assertThat(env.payload()).isNotNull();
        assertThat(env.payload().get("reason").asText()).isEqualTo("TIMEOUT");
    }

    @Test
    void firstGeneratesRandomEventIds() {
        assertThat(firstCancelled().eventId()).isNotEqualTo(firstCancelled().eventId());
    }

    @Test
    void nextCopiesOrderAndCorrelationAndSetsCausation() {
        EventEnvelope cause = firstCancelled();

        EventEnvelope next = Envelopes.next(cause, "DeliveryFailed",
                new DeliveryFailed(DeliveryFailureReason.NO_DRIVER_AVAILABLE));

        assertThat(next.orderId()).isEqualTo(cause.orderId());
        assertThat(next.correlationId()).isEqualTo(cause.correlationId());
        assertThat(next.causationId()).isEqualTo(cause.eventId());
        assertThat(next.eventId()).isNotEqualTo(cause.eventId());
        assertThat(next.eventType()).isEqualTo("DeliveryFailed");
        assertThat(next.eventVersion()).isEqualTo(1);
    }

    @Test
    void nextChainKeepsCorrelationAndLinksCausation() {
        EventEnvelope a = firstCancelled();
        EventEnvelope b = Envelopes.next(a, "DeliveryFailed", new DeliveryFailed(DeliveryFailureReason.DRIVER_CANCELLED));
        EventEnvelope c = Envelopes.next(b, "OrderCancelled", new OrderCancelled(CancelReason.NO_DRIVER));

        assertThat(c.causationId()).isEqualTo(b.eventId());
        assertThat(c.correlationId()).isEqualTo(correlationId);
        assertThat(c.orderId()).isEqualTo(orderId);
        assertThat(c.eventId()).isNotIn(a.eventId(), b.eventId());
    }

    @Test
    void firstRejectsUnknownType() {
        assertThatThrownBy(() -> Envelopes.first("Nope", orderId, correlationId, new OrderCancelled(CancelReason.TIMEOUT)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void firstRejectsPayloadThatDoesNotMatchType() {
        assertThatThrownBy(() -> Envelopes.first("OrderPlaced", orderId, correlationId,
                new OrderCancelled(CancelReason.TIMEOUT)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void nextRejectsUnknownTypeAndMismatchedPayload() {
        EventEnvelope cause = firstCancelled();

        assertThatThrownBy(() -> Envelopes.next(cause, "Nope", new OrderCancelled(CancelReason.TIMEOUT)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Envelopes.next(cause, "DeliveryFailed", new OrderCancelled(CancelReason.TIMEOUT)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void generatedEnvelopeSurvivesCodecRoundTrip() {
        EventEnvelope env = Envelopes.next(firstCancelled(), "DeliveryFailed",
                new DeliveryFailed(DeliveryFailureReason.NO_DRIVER_AVAILABLE));

        EventEnvelope decoded = EventCodec.fromJson(EventCodec.toJson(env));

        assertThat(decoded.eventId()).isEqualTo(env.eventId());
        assertThat(decoded.causationId()).isEqualTo(env.causationId());
        assertThat(decoded.correlationId()).isEqualTo(env.correlationId());
        assertThat(EventCodec.payloadOf(decoded))
                .isEqualTo(new DeliveryFailed(DeliveryFailureReason.NO_DRIVER_AVAILABLE));
    }

    @Test
    void headersHoldEventTypeVersionAndCorrelationAsUtf8Strings() {
        EventEnvelope env = firstCancelled();

        Map<String, byte[]> headers = EventHeaders.from(env);

        assertThat(headers).containsOnlyKeys("eventType", "eventVersion", "correlationId");
        assertThat(new String(headers.get("eventType"), StandardCharsets.UTF_8)).isEqualTo(env.eventType());
        assertThat(new String(headers.get("eventVersion"), StandardCharsets.UTF_8))
                .isEqualTo(String.valueOf(env.eventVersion()));
        assertThat(new String(headers.get("correlationId"), StandardCharsets.UTF_8))
                .isEqualTo(env.correlationId().toString());
    }
}
