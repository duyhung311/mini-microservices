package com.example.fooddelivery.delivery.messaging;

import com.example.fooddelivery.delivery.domain.DeliveryService;
import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventCodec;
import com.example.fooddelivery.events.EventPayload;
import com.example.fooddelivery.events.InvalidEventException;
import com.example.fooddelivery.events.OrderAccepted;
import com.example.fooddelivery.events.OrderReady;
import com.example.fooddelivery.events.Topics;
import com.example.fooddelivery.events.tracing.TraceLinks;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

/** Consumes restaurant.events: OrderAccepted (assign a driver or fail) and OrderReady (pickup guard). */
@Component
public class RestaurantEventsListener {

    private static final Logger log = LoggerFactory.getLogger(RestaurantEventsListener.class);

    private final DeliveryService service;
    private final MeterRegistry registry;

    public RestaurantEventsListener(DeliveryService service, MeterRegistry registry) {
        this.service = service;
        this.registry = registry;
    }

    @RetryableTopic(
            attempts = "${app.kafka.retry.attempts:4}",
            backoff = @Backoff(delayExpression = "${app.kafka.retry.delay-ms:1000}",
                    multiplierExpression = "${app.kafka.retry.multiplier:5}",
                    maxDelayExpression = "${app.kafka.retry.max-delay-ms:30000}"),
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            retryTopicSuffix = "." + Topics.DELIVERY_SERVICE + ".retry",
            dltTopicSuffix = "." + Topics.DELIVERY_SERVICE + ".dlt",
            autoCreateTopics = "false",
            exclude = {InvalidEventException.class},
            dltStrategy = DltStrategy.FAIL_ON_ERROR)
    @KafkaListener(topics = Topics.RESTAURANT_EVENTS, groupId = Topics.DELIVERY_SERVICE)
    public void onMessage(ConsumerRecord<String, String> record) {
        EventEnvelope env = EventDecoding.decode(record);
        try (TraceLinks.Binding ignored = TraceLinks.bindCorrelation(env.correlationId())) {
            handle(record, env);
        }
    }

    private void handle(ConsumerRecord<String, String> record, EventEnvelope env) {
        EventPayload payload = EventCodec.payloadOf(env);
        if (payload instanceof OrderAccepted accepted) {
            service.onOrderAccepted(env, accepted);
        } else if (payload instanceof OrderReady) {
            service.onOrderReady(env);
        } else {
            log.debug("Skipping {} on {}", env.eventType(), record.topic());
        }
    }

    @DltHandler
    public void onDlt(ConsumerRecord<String, String> record) {
        EventDecoding.alert(log, registry, record);
    }
}
