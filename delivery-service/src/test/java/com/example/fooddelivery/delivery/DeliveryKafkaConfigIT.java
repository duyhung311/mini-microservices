package com.example.fooddelivery.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.delivery.support.DeliveryIntegrationTestBase;
import com.example.fooddelivery.events.Topics;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListenerContainer;

/** Criterion 17: producer, consumer and listener container settings; subscribed topics match {@link Topics}. */
class DeliveryKafkaConfigIT extends DeliveryIntegrationTestBase {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    ConsumerFactory<String, String> consumerFactory;

    @Autowired
    KafkaListenerEndpointRegistry registry;

    @Autowired
    Environment environment;

    @Test
    void producerIsIdempotentAndAcksAll() {
        Map<String, Object> props = kafkaTemplate.getProducerFactory().getConfigurationProperties();
        assertThat(String.valueOf(props.get("acks"))).isEqualTo("all");
        assertThat(String.valueOf(props.get("enable.idempotence"))).isEqualTo("true");
    }

    @Test
    void consumerUsesServiceGroupEarliestAndManualCommit() {
        Map<String, Object> props = consumerFactory.getConfigurationProperties();
        assertThat(String.valueOf(props.get("group.id"))).isEqualTo("delivery-service");
        assertThat(String.valueOf(props.get("auto.offset.reset"))).isEqualTo("earliest");
        assertThat(String.valueOf(props.get("enable.auto.commit"))).isEqualTo("false");
    }

    @Test
    void listenerContainersAckRecord() {
        assertThat(registry.getListenerContainers()).isNotEmpty();
        for (MessageListenerContainer container : registry.getListenerContainers()) {
            assertThat(container.getContainerProperties().getAckMode())
                    .as("ack mode of %s", container.getListenerId()).isEqualTo(ContainerProperties.AckMode.RECORD);
        }
    }

    /** Main containers use exactly the service group; Spring derives retry/DLT groups from it as a prefix. */
    @Test
    void mainListenerContainersUseServiceGroup() {
        int mains = 0;
        for (MessageListenerContainer container : registry.getListenerContainers()) {
            String[] topics = container.getContainerProperties().getTopics();
            boolean main = topics != null && topics.length == 1
                    && (Topics.ORDER_EVENTS.equals(topics[0]) || Topics.RESTAURANT_EVENTS.equals(topics[0]));
            if (main) {
                mains++;
                assertThat(container.getGroupId()).isEqualTo("delivery-service");
            } else {
                assertThat(container.getGroupId()).as("group of %s", container.getListenerId())
                        .startsWith("delivery-service");
            }
        }
        assertThat(mains).isEqualTo(2);
    }

    @Test
    void listenerContainersSubscribeToExactlyMainRetryAndDltTopics() {
        Set<String> subscribed = new HashSet<>();
        for (MessageListenerContainer container : registry.getListenerContainers()) {
            String[] topics = container.getContainerProperties().getTopics();
            assertThat(topics).as("topics of %s", container.getListenerId()).isNotNull().isNotEmpty();
            subscribed.addAll(Set.of(topics));
        }
        Set<String> expected = new HashSet<>();
        for (Topics.Subscription s : Topics.subscriptions()) {
            if (!s.group().equals(Topics.DELIVERY_SERVICE)) {
                continue;
            }
            expected.add(s.topic());
            for (int i = 0; i < Topics.RETRY_HOPS; i++) {
                expected.add(Topics.retry(s.topic(), s.group(), i));
            }
            expected.add(Topics.dlt(s.topic(), s.group()));
        }
        assertThat(expected).hasSize(10);
        assertThat(subscribed).isEqualTo(expected);
    }

    @Test
    void schemaIsValidatedNotGeneratedByHibernate() {
        assertThat(environment.getProperty("spring.jpa.hibernate.ddl-auto")).isEqualTo("validate");
    }
}
