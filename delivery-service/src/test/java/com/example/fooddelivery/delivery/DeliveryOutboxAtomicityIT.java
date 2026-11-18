package com.example.fooddelivery.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.springframework.test.util.AopTestUtils.getUltimateTargetObject;

import com.example.fooddelivery.delivery.domain.DeliveryService;
import com.example.fooddelivery.delivery.outbox.OutboxEventPublisher;
import com.example.fooddelivery.delivery.support.DeliveryIntegrationTestBase;
import com.example.fooddelivery.delivery.support.KafkaProbe;
import com.example.fooddelivery.events.Envelopes;
import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.OrderAccepted;
import com.example.fooddelivery.events.OrderPlaced;
import com.example.fooddelivery.events.Topics;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

/** Criterion 14: the business change and its outbox row commit or roll back together. */
class DeliveryOutboxAtomicityIT extends DeliveryIntegrationTestBase {

    @MockitoSpyBean
    OutboxEventPublisher outboxPublisher;

    @Autowired
    DeliveryService deliveryService;

    @Autowired
    MeterRegistry meterRegistry;

    @Value("${spring.kafka.bootstrap-servers}")
    String bootstrap;

    private long count(String sql, Object... args) {
        Long n = jdbc.queryForObject(sql, Long.class, args);
        return n == null ? 0 : n;
    }

    private OrderPlaced placedPayload() {
        return new OrderPlaced(UUID.randomUUID(), RESTAURANT,
                List.of(new OrderPlaced.Item(UUID.randomUUID(), 1, new BigDecimal("12.50"))),
                new BigDecimal("12.50"), DROPOFF_ADDRESS);
    }

    private void failOutboxFor(UUID orderId) {
        doThrow(new IllegalStateException("outbox insert failed"))
                .when((OutboxEventPublisher) getUltimateTargetObject(outboxPublisher)).publish(argThat(e -> e != null && orderId.equals(e.orderId())));
    }

    @Test
    void failingOutboxInsertRollsBackDriverAssignment() {
        UUID orderId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        EventEnvelope placed = Envelopes.first(EventTypes.ORDER_PLACED, orderId, correlationId, placedPayload());
        deliveryService.onOrderPlaced(placed, (OrderPlaced) EventCodec.payloadOf(placed));
        assertThat(deliveryStatus(orderId)).isEqualTo("PENDING");

        EventEnvelope accepted = Envelopes.next(placed, EventTypes.ORDER_ACCEPTED,
                new OrderAccepted(RESTAURANT, 15, PICKUP_ADDRESS));
        failOutboxFor(orderId);
        assertThatThrownBy(() -> deliveryService.onOrderAccepted(accepted,
                (OrderAccepted) EventCodec.payloadOf(accepted)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("outbox insert failed");

        assertThat(deliveryStatus(orderId)).isEqualTo("PENDING");
        assertThat(deliveryDriverId(orderId)).isNull();
        assertThat(count("SELECT count(*) FROM drivers WHERE status <> 'AVAILABLE'")).isZero();
        assertThat(count("SELECT count(*) FROM outbox WHERE aggregate_id = ?", orderId)).isZero();
        assertThat(count("SELECT count(*) FROM processed_events WHERE event_id = ?", accepted.eventId())).isZero();
        assertThat(count("SELECT count(*) FROM processed_events WHERE event_id = ?", placed.eventId())).isEqualTo(1);
    }

    @Test
    void failingOutboxInsertRollsBackNoDriverFailure() {
        setAllDrivers("OFFLINE");
        UUID orderId = UUID.randomUUID();
        EventEnvelope accepted = Envelopes.first(EventTypes.ORDER_ACCEPTED, orderId, UUID.randomUUID(),
                new OrderAccepted(RESTAURANT, 15, PICKUP_ADDRESS));
        failOutboxFor(orderId);

        assertThatThrownBy(() -> deliveryService.onOrderAccepted(accepted,
                (OrderAccepted) EventCodec.payloadOf(accepted)))
                .isInstanceOf(IllegalStateException.class);

        assertThat(count("SELECT count(*) FROM deliveries WHERE order_id = ?", orderId)).isZero();
        assertThat(count("SELECT count(*) FROM outbox WHERE aggregate_id = ?", orderId)).isZero();
        assertThat(count("SELECT count(*) FROM processed_events WHERE event_id = ?", accepted.eventId())).isZero();
    }

    @Test
    void successCommitsBusinessRowAndOutboxRowAndRelayPublishesThem() {
        UUID orderId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        EventEnvelope placed = sendPlaced(orderId, correlationId);
        EventEnvelope accepted = sendAccepted(placed);
        awaitDeliveryStatus(orderId, "ASSIGNED");

        // committed together: the ASSIGNED row is visible only together with its outbox row
        assertThat(count("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND event_type = ?", orderId,
                EventTypes.DRIVER_ASSIGNED)).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM drivers WHERE status = 'BUSY'")).isEqualTo(1);

        await().atMost(WAIT).pollInterval(POLL).until(() -> count(
                "SELECT count(*) FROM outbox WHERE aggregate_id = ? AND published_at IS NOT NULL", orderId) == 1);

        List<Published> published = awaitDeliveryEvents(orderId, EventTypes.DRIVER_ASSIGNED);
        assertThat(published).hasSize(1);
        assertEnvelope(published.get(0), EventTypes.DRIVER_ASSIGNED, orderId, correlationId, accepted.eventId());

        // the relay publishes the compact canonical JSON of the stored envelope
        String stored = jdbc.queryForObject("SELECT payload::text FROM outbox WHERE aggregate_id = ?", String.class,
                orderId);
        String expected = EventCodec.toJson(EventCodec.fromJson(stored));
        List<KafkaProbe.Rec> raw = new KafkaProbe(bootstrap).read(Topics.DELIVERY_EVENTS, orderId.toString());
        assertThat(raw).hasSize(1);
        assertThat(raw.get(0).value()).isEqualTo(expected);
        assertThat(raw.get(0).headers()).containsEntry("eventType", EventTypes.DRIVER_ASSIGNED)
                .containsEntry("correlationId", correlationId.toString());
    }

    @Test
    void outboxPendingGaugeIsRegistered() {
        assertThat(meterRegistry.find("outbox.pending").gauge()).isNotNull();
    }
}
