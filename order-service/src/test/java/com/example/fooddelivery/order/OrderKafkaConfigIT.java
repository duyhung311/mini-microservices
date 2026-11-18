package com.example.fooddelivery.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.events.Topics;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListenerContainer;

/** Criterion 17: producer and consumer settings and listener containers of the running context. */
class OrderKafkaConfigIT extends AbstractOrderIT {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ConsumerFactory<?, ?> consumerFactory;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Test
    void producerIsIdempotentAndAcksAll() {
        Map<String, Object> cfg = kafkaTemplate.getProducerFactory().getConfigurationProperties();
        assertThat(String.valueOf(cfg.get("acks"))).isEqualTo("all");
        assertThat(String.valueOf(cfg.get("enable.idempotence"))).isEqualTo("true");
    }

    @Test
    void consumerGroupOffsetResetAndAutoCommit() {
        Map<String, Object> cfg = consumerFactory.getConfigurationProperties();
        assertThat(String.valueOf(cfg.get("group.id"))).isEqualTo("order-service");
        assertThat(String.valueOf(cfg.get("auto.offset.reset"))).isEqualTo("earliest");
        assertThat(String.valueOf(cfg.get("enable.auto.commit"))).isEqualTo("false");
    }

    @Test
    void everyListenerContainerUsesAckModeRecordAndGroupOrderService() {
        Collection<MessageListenerContainer> containers = registry.getListenerContainers();
        assertThat(containers).isNotEmpty();
        for (MessageListenerContainer c : containers) {
            assertThat(c.getContainerProperties().getAckMode()).as("ack mode of %s", c.getListenerId())
                    .isEqualTo(ContainerProperties.AckMode.RECORD);
            // Spring Kafka derives the group id of retry/DLT containers from the main group id (e.g.
            // "order-service.order-service.dlt"); the main-topic containers use the group id itself.
            assertThat(c.getGroupId()).as("group of %s", c.getListenerId()).startsWith("order-service");
            List<String> topics = java.util.Arrays.asList(c.getContainerProperties().getTopics());
            if (topics.contains(Topics.RESTAURANT_EVENTS) || topics.contains(Topics.DELIVERY_EVENTS)) {
                assertThat(c.getGroupId()).as("group of main container %s", c.getListenerId())
                        .isEqualTo("order-service");
            }
        }
    }

    @Test
    void subscribedTopicsEqualTheTopicsConstantsForThisGroup() {
        Set<String> expected = new TreeSet<>();
        for (Topics.Subscription s : Topics.subscriptions()) {
            if (Topics.ORDER_SERVICE.equals(s.group())) {
                expected.add(s.topic());
                for (int i = 0; i < Topics.RETRY_HOPS; i++) {
                    expected.add(Topics.retry(s.topic(), s.group(), i));
                }
                expected.add(Topics.dlt(s.topic(), s.group()));
            }
        }
        assertThat(expected).hasSize(10);

        Set<String> actual = new TreeSet<>();
        List<String> perContainer = new ArrayList<>();
        for (MessageListenerContainer c : registry.getListenerContainers()) {
            String[] topics = c.getContainerProperties().getTopics();
            assertThat(topics).as("topics of %s", c.getListenerId()).isNotNull();
            for (String t : topics) {
                actual.add(t);
                perContainer.add(t);
            }
        }
        assertThat(actual).isEqualTo(expected);
        // no topic is subscribed by two containers
        assertThat(perContainer).hasSameSizeAs(actual);
    }
}
