package com.example.fooddelivery.delivery.messaging;

import java.util.function.Consumer;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.retrytopic.DeadLetterPublishingRecovererFactory;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationSupport;

/**
 * Retry and DLT topics have fewer partitions than the source topics (DLT: 1), so the publisher must not pin the
 * source partition. A resolver that returns null lets Kafka choose the partition from the record key.
 */
@Configuration
public class RetryTopicConfig extends RetryTopicConfigurationSupport {

    @Override
    protected Consumer<DeadLetterPublishingRecovererFactory> configureDeadLetterPublishingContainerFactory() {
        return factory -> factory.setPartitionResolver((cr, nextTopic) -> null);
    }
}
