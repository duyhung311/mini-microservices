package com.example.fooddelivery.order.messaging;

import java.util.function.Consumer;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.retrytopic.DeadLetterPublishingRecovererFactory;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationSupport;

/**
 * Retry and DLT records are published without an explicit partition, so the producer chooses it by key. The default
 * resolver would reuse the source partition, which does not exist on the 1-partition DLT for partitions 1 and 2.
 */
@Configuration(proxyBeanMethods = false)
public class RetryTopicConfig extends RetryTopicConfigurationSupport {

    @Override
    protected Consumer<DeadLetterPublishingRecovererFactory> configureDeadLetterPublishingContainerFactory() {
        return factory -> factory.setPartitionResolver((record, nextTopic) -> null);
    }
}
