package com.example.fooddelivery.order.messaging;

import com.example.fooddelivery.events.InvalidEventException;
import com.example.fooddelivery.events.Topics;
import com.example.fooddelivery.order.domain.OrderService;
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

/** Consumes delivery.events (DriverAssigned, DeliveryFailed, OrderPickedUp, OrderDelivered), with retry topics and a DLT. */
@Component
public class DeliveryEventsListener {

    private static final Logger log = LoggerFactory.getLogger(DeliveryEventsListener.class);

    private final OrderService orderService;
    private final MeterRegistry registry;

    public DeliveryEventsListener(OrderService orderService, MeterRegistry registry) {
        this.orderService = orderService;
        this.registry = registry;
    }

    @RetryableTopic(
            attempts = "${app.kafka.retry.attempts:4}",
            backoff = @Backoff(
                    delayExpression = "${app.kafka.retry.delay-ms:1000}",
                    multiplierExpression = "${app.kafka.retry.multiplier:5}",
                    maxDelayExpression = "${app.kafka.retry.max-delay-ms:30000}"),
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            retryTopicSuffix = "." + Topics.ORDER_SERVICE + ".retry",
            dltTopicSuffix = "." + Topics.ORDER_SERVICE + ".dlt",
            autoCreateTopics = "false",
            exclude = {InvalidEventException.class},
            dltStrategy = DltStrategy.FAIL_ON_ERROR)
    @KafkaListener(topics = Topics.DELIVERY_EVENTS, groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(ConsumerRecord<String, String> record) {
        EventDispatch.dispatch(record, orderService);
    }

    @DltHandler
    public void onDlt(ConsumerRecord<String, String> record) {
        EventDispatch.alertDlt(record, registry, log);
    }
}
