package com.example.fooddelivery.restaurant.messaging;

import java.util.function.Consumer;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.retrytopic.DeadLetterPublishingRecovererFactory;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationSupport;

/**
 * Retry and DLT records are published with a partition resolver that returns null, so the partition is chosen by
 * key (the DLT has one partition, the source topic has three). Spring Kafka's default would reuse the source
 * partition number, which does not exist on the DLT.
 */
@Configuration
public class RetryTopicConfig extends RetryTopicConfigurationSupport {

    @Override
    protected Consumer<DeadLetterPublishingRecovererFactory> configureDeadLetterPublishingContainerFactory() {
        return factory -> factory.setPartitionResolver((record, nextTopic) -> null);
    }
}
