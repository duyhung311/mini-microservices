package com.example.fooddelivery.restaurant;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.Topics;
import com.example.fooddelivery.restaurant.simulation.TicketAutoAdvancer;
import com.example.fooddelivery.restaurant.support.RestaurantIntegrationTestBase;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

/**
 * Criterion 20, restaurant part. Each nested class has its own Spring context; both are discarded
 * afterwards so that no scheduler or extra consumer outlives them.
 */
class RestaurantAutoAdvanceIT {

    @Nested
    @TestPropertySource(properties = {
            "simulation.auto-advance=true",
            "simulation.prep-delay=PT1S",
            "simulation.tick=PT0.2S"})
    @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
    class AutoAdvanceEnabled extends RestaurantIntegrationTestBase {

        @Autowired
        ApplicationContext context;

        @Test
        void acceptedTicketReachesReadyOnItsOwnAndOrderReadyIsPublished() {
            assertThat(context.getBeansOfType(TicketAutoAdvancer.class)).hasSize(1);

            EventEnvelope placed = placeOrder(PASTA_PLACE, MARGHERITA);

            awaitTicketStatus(placed.orderId(), "READY");
            List<Published> ready = awaitEvents(Topics.RESTAURANT_EVENTS, placed.orderId(),
                    EventTypes.ORDER_READY);
            Published record = ready.get(0);
            assertThat(record.key()).isEqualTo(placed.orderId().toString());
            // scheduler-originated: causationId null, correlationId kept
            assertThat(record.envelope().causationId()).isNull();
            assertThat(record.envelope().correlationId()).isEqualTo(placed.correlationId());
            assertThat(record.headers().get("correlationId")).isEqualTo(placed.correlationId().toString());
            assertThat(record.headers().get("eventType")).isEqualTo("OrderReady");
        }

        @Test
        void readyTicketIsAdvancedOnlyOnce() {
            EventEnvelope placed = placeOrder(PASTA_PLACE, MARGHERITA);
            awaitTicketStatus(placed.orderId(), "READY");
            assertTicketStaysAt(placed.orderId(), "READY");
            assertThat(eventsOfType(Topics.RESTAURANT_EVENTS, placed.orderId(), EventTypes.ORDER_READY))
                    .hasSize(1);
        }
    }

    @Nested
    @TestPropertySource(properties = {
            "simulation.prep-delay=PT0.5S",
            "simulation.tick=PT0.2S"})
    @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
    class AutoAdvanceDisabled extends RestaurantIntegrationTestBase {

        @Autowired
        ApplicationContext context;

        @Test
        void nothingAdvancesWhenAutoAdvanceIsOff() {
            // auto-advance is not set, so it keeps its default (false), although prep delay and tick are tiny
            assertThat(context.getBeansOfType(TicketAutoAdvancer.class)).isEmpty();

            EventEnvelope placed = placeOrder(PASTA_PLACE, MARGHERITA);
            awaitTicketStatus(placed.orderId(), "ACCEPTED");

            assertTicketStaysAt(placed.orderId(), "ACCEPTED");
            assertThat(eventsOfType(Topics.RESTAURANT_EVENTS, placed.orderId(), EventTypes.ORDER_READY)).isEmpty();
        }
    }
}
