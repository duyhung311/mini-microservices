package com.example.fooddelivery.restaurant;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.events.Topics;
import com.example.fooddelivery.restaurant.support.RestaurantIntegrationTestBase;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListenerContainer;

/** Criterion 17: producer and consumer client settings, AckMode RECORD, and the subscribed topics. */
class RestaurantKafkaConfigIT extends RestaurantIntegrationTestBase {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ConsumerFactory<String, String> consumerFactory;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Test
    void producerIsIdempotentAndAcksAll() {
        Map<String, Object> config = kafkaTemplate.getProducerFactory().getConfigurationProperties();
        assertThat(String.valueOf(config.get("acks"))).isEqualTo("all");
        assertThat(String.valueOf(config.get("enable.idempotence"))).isEqualTo("true");
    }

    @Test
    void consumerGroupOffsetResetAndAutoCommit() {
        Map<String, Object> config = consumerFactory.getConfigurationProperties();
        assertThat(String.valueOf(config.get("group.id"))).isEqualTo("restaurant-service");
        assertThat(String.valueOf(config.get("auto.offset.reset"))).isEqualTo("earliest");
        assertThat(String.valueOf(config.get("enable.auto.commit"))).isEqualTo("false");
    }

    @Test
    void everyListenerContainerUsesAckModeRecord() {
        Collection<MessageListenerContainer> containers = registry.getAllListenerContainers();
        assertThat(containers).isNotEmpty();
        for (MessageListenerContainer container : containers) {
            assertThat(container.getContainerProperties().getAckMode())
                    .as("ack mode of %s", container.getListenerId()).isEqualTo(ContainerProperties.AckMode.RECORD);
        }
    }

    @Test
    void mainTopicContainersUseTheServiceGroupAndRetryContainersAreDerivedFromIt() {
        for (MessageListenerContainer container : registry.getAllListenerContainers()) {
            String[] topics = container.getContainerProperties().getTopics();
            assertThat(topics).isNotNull();
            boolean main = Set.of(topics).stream()
                    .anyMatch(t -> t.equals(Topics.ORDER_EVENTS) || t.equals(Topics.DELIVERY_EVENTS));
            if (main) {
                assertThat(container.getGroupId()).as("group of %s", container.getListenerId())
                        .isEqualTo(Topics.RESTAURANT_SERVICE);
            } else {
                assertThat(container.getGroupId()).as("group of %s", container.getListenerId())
                        .startsWith(Topics.RESTAURANT_SERVICE);
            }
        }
    }

    @Test
    void subscribedTopicsAreMainRetryAndDltTopicsOfThisGroup() {
        Set<String> expected = new HashSet<>();
        for (Topics.Subscription s : Topics.subscriptions()) {
            if (!Topics.RESTAURANT_SERVICE.equals(s.group())) {
                continue;
            }
            expected.add(s.topic());
            for (int i = 0; i < Topics.RETRY_HOPS; i++) {
                expected.add(Topics.retry(s.topic(), s.group(), i));
            }
            expected.add(Topics.dlt(s.topic(), s.group()));
        }
        assertThat(expected).hasSize(10);

        Set<String> actual = new HashSet<>();
        for (MessageListenerContainer container : registry.getAllListenerContainers()) {
            String[] topics = container.getContainerProperties().getTopics();
            assertThat(topics).as("topics of %s (no pattern subscriptions)", container.getListenerId()).isNotNull();
            actual.addAll(Set.of(topics));
        }
        assertThat(actual).isEqualTo(expected);
    }
}
