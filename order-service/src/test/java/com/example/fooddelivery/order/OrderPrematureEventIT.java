package com.example.fooddelivery.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.Topics;
import java.time.Duration;
import java.util.UUID;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Criteria 11/12/18: a PREMATURE event (DriverAssigned while the order is still PLACED) travels through the retry
 * topics and is applied once OrderAccepted has arrived, so the order converges. OrderAccepted is only produced after
 * the DriverAssigned record has been observed on delivery.events.order-service.retry-0.
 *
 * Timing: with the module's shrunk delays (200 ms x2, max 1000 ms, attempts left at the default 4) the attempts happen
 * about 0.2 s, 0.6 s and 1.4 s after the first one. The tap sees the retry-0 record within ~100 ms and OrderAccepted
 * is applied within a few tens of ms after that, i.e. before the second retry attempt. Attempts are not raised
 * because only retry-0..2 exist (attempts - 1 retry topics, auto-create is off).
 */
class OrderPrematureEventIT extends AbstractOrderIT {

    @Autowired
    private JdbcTemplate jdbc;

    @Test
    void driverAssignedBeforeOrderAcceptedIsRetriedAndConverges() {
        TopicTap tap = TopicTap.instance();
        String retry0 = Topics.retry(Topics.DELIVERY_EVENTS, Topics.ORDER_SERVICE, 0);
        String dlt = Topics.dlt(Topics.DELIVERY_EVENTS, Topics.ORDER_SERVICE);

        UUID orderId = UUID.fromString(placeOrder().get("orderId").asText());
        EventEnvelope placed = awaitPlaced(orderId);
        UUID driver = UUID.randomUUID();
        EventEnvelope accepted = SagaEvents.accepted(placed);
        EventEnvelope assigned = SagaEvents.driverAssigned(accepted, driver);

        // DriverAssigned first: the order is still PLACED, so it is PREMATURE
        send(Topics.DELIVERY_EVENTS, assigned);
        Awaitility.await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(20))
                .until(() -> tap.count(retry0, orderId) >= 1);
        assertThat(tap.seen(retry0, orderId).get(0).value()).contains(assigned.eventId().toString());
        // the premature event changed nothing yet and is not recorded as processed (its transaction rolled back)
        assertThat(status(orderId)).isEqualTo("PLACED");
        assertThat(jdbc.queryForObject("select count(*) from processed_events where event_id = ?", Integer.class,
                assigned.eventId())).isZero();

        // only now does the cause arrive
        send(Topics.RESTAURANT_EVENTS, accepted);

        awaitStatus(orderId, "DRIVER_ASSIGNED");
        assertThat(history(orderId)).isEqualTo(historyOf("PLACED", "ACCEPTED", "DRIVER_ASSIGNED"));
        assertThat(get("/orders/" + orderId).getBody().get("driverId").asText()).isEqualTo(driver.toString());
        assertThat(jdbc.queryForObject("select count(*) from processed_events where event_id = ?", Integer.class,
                assigned.eventId())).isEqualTo(1);
        // converged without dead-lettering
        Awaitility.await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(tap.count(dlt, orderId)).isZero());
        assertThat(status(orderId)).isEqualTo("DRIVER_ASSIGNED");
    }
}
