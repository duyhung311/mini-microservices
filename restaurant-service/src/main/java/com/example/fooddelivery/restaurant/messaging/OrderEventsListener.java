package com.example.fooddelivery.restaurant.messaging;

import com.example.fooddelivery.events.EventEnvelope;
import com.example.fooddelivery.events.EventTypes;
import com.example.fooddelivery.events.InvalidEventException;
import com.example.fooddelivery.events.Topics;
import com.example.fooddelivery.events.tracing.TraceLinks;
import com.example.fooddelivery.restaurant.domain.TicketService;
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

@Component
public class OrderEventsListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventsListener.class);

    private final TicketService service;
    private final MeterRegistry meters;

    public OrderEventsListener(TicketService service, MeterRegistry meters) {
        this.service = service;
        this.meters = meters;
    }

    @RetryableTopic(attempts = "${app.kafka.retry.attempts:4}",
            backoff = @Backoff(delayExpression = "${app.kafka.retry.delay-ms:1000}",
                    multiplierExpression = "${app.kafka.retry.multiplier:5}",
                    maxDelayExpression = "${app.kafka.retry.max-delay-ms:30000}"),
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            retryTopicSuffix = ".restaurant-service.retry", dltTopicSuffix = ".restaurant-service.dlt",
            autoCreateTopics = "false", exclude = {InvalidEventException.class},
            dltStrategy = DltStrategy.FAIL_ON_ERROR)
    @KafkaListener(topics = Topics.ORDER_EVENTS, groupId = Topics.RESTAURANT_SERVICE)
    public void onMessage(ConsumerRecord<String, String> record) {
        EventEnvelope envelope = EventDecoding.decode(record);
        try (TraceLinks.Binding ignored = TraceLinks.bindCorrelation(envelope.correlationId())) {
            handle(record, envelope);
        }
    }

    private void handle(ConsumerRecord<String, String> record, EventEnvelope envelope) {
        if (EventTypes.ORDER_PLACED.equals(envelope.eventType())) {
            service.handleOrderPlaced(envelope);
        } else if (EventTypes.ORDER_CANCELLED.equals(envelope.eventType())) {
            service.handleOrderCancelled(envelope);
        } else {
            log.debug("Skipping {} on {}", envelope.eventType(), record.topic());
        }
    }

    @DltHandler
    public void onDlt(ConsumerRecord<String, String> record) {
        log.error("ALERT dlt topic={} key={} eventId={}", record.topic(), record.key(),
                EventDecoding.eventIdOrNa(record.value()));
        meters.counter("kafka.dlt.records", "topic", record.topic()).increment();
    }
}
