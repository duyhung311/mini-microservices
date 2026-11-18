package com.example.fooddelivery.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.events.InvalidEventException;
import com.example.fooddelivery.events.Topics;
import com.example.fooddelivery.order.messaging.DeliveryEventsListener;
import com.example.fooddelivery.order.messaging.RestaurantEventsListener;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;

/**
 * Criterion 18 (default policy): 4 attempts at 1000 / 5000 / 25000 ms, read from the annotations, no Spring context.
 */
class OrderRetryBackoffTest {

    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{[^:}]+:([^}]*)}");

    private static long defaultOf(String expression) {
        Matcher m = PLACEHOLDER.matcher(expression);
        assertThat(m.matches()).as("placeholder with default: %s", expression).isTrue();
        return Long.parseLong(m.group(1));
    }

    private static Method retryableMethod(Class<?> listener) {
        for (Method m : listener.getDeclaredMethods()) {
            if (m.isAnnotationPresent(RetryableTopic.class)) {
                return m;
            }
        }
        throw new AssertionError("no @RetryableTopic method on " + listener);
    }

    @ParameterizedTest
    @ValueSource(strings = {Topics.RESTAURANT_EVENTS, Topics.DELIVERY_EVENTS})
    void productionDefaults(String sourceTopic) {
        Class<?> listener = Topics.RESTAURANT_EVENTS.equals(sourceTopic) ? RestaurantEventsListener.class
                : DeliveryEventsListener.class;
        Method method = retryableMethod(listener);
        RetryableTopic rt = method.getAnnotation(RetryableTopic.class);

        assertThat(method.getAnnotation(KafkaListener.class).topics()).containsExactly(sourceTopic);

        long attempts = defaultOf(rt.attempts());
        long delay = defaultOf(rt.backoff().delayExpression());
        long multiplier = defaultOf(rt.backoff().multiplierExpression());
        long max = defaultOf(rt.backoff().maxDelayExpression());
        assertThat(attempts).isEqualTo(4);
        assertThat(multiplier).isEqualTo(5);
        assertThat(max).isEqualTo(30000);

        List<Long> delays = new ArrayList<>();
        long d = delay;
        for (int i = 0; i < attempts - 1; i++) {
            delays.add(Math.min(d, max));
            d = d * multiplier;
        }
        assertThat(delays).containsExactly(1000L, 5000L, 25000L);
    }

    @ParameterizedTest
    @ValueSource(strings = {Topics.RESTAURANT_EVENTS, Topics.DELIVERY_EVENTS})
    void topicNamingAndPolicyFlags(String sourceTopic) {
        Class<?> listener = Topics.RESTAURANT_EVENTS.equals(sourceTopic) ? RestaurantEventsListener.class
                : DeliveryEventsListener.class;
        RetryableTopic rt = retryableMethod(listener).getAnnotation(RetryableTopic.class);

        assertThat(rt.retryTopicSuffix()).isEqualTo(".order-service.retry");
        assertThat(rt.dltTopicSuffix()).isEqualTo(".order-service.dlt");
        assertThat(rt.topicSuffixingStrategy()).isEqualTo(TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE);
        assertThat(rt.autoCreateTopics()).isEqualTo("false");
        assertThat(rt.exclude()).contains(InvalidEventException.class);
        assertThat(rt.dltStrategy()).isEqualTo(DltStrategy.FAIL_ON_ERROR);

        // the suffixes must produce exactly the names the Topics constants (and kafka-init) create
        for (int i = 0; i < Topics.RETRY_HOPS; i++) {
            assertThat(sourceTopic + rt.retryTopicSuffix() + "-" + i)
                    .isEqualTo(Topics.retry(sourceTopic, Topics.ORDER_SERVICE, i));
        }
        assertThat(sourceTopic + rt.dltTopicSuffix()).isEqualTo(Topics.dlt(sourceTopic, Topics.ORDER_SERVICE));
    }

    @ParameterizedTest
    @ValueSource(strings = {Topics.RESTAURANT_EVENTS, Topics.DELIVERY_EVENTS})
    void exactlyOneDltHandlerPerListener(String sourceTopic) {
        Class<?> listener = Topics.RESTAURANT_EVENTS.equals(sourceTopic) ? RestaurantEventsListener.class
                : DeliveryEventsListener.class;
        int handlers = 0;
        for (Method m : listener.getDeclaredMethods()) {
            if (m.isAnnotationPresent(DltHandler.class)) {
                handlers++;
            }
        }
        assertThat(handlers).isEqualTo(1);
    }
}
