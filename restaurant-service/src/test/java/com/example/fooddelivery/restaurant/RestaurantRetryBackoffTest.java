package com.example.fooddelivery.restaurant;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.events.InvalidEventException;
import com.example.fooddelivery.events.Topics;
import com.example.fooddelivery.restaurant.messaging.DeliveryEventsListener;
import com.example.fooddelivery.restaurant.messaging.OrderEventsListener;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;

/** Criterion 18 (defaults part): production retry policy read from the annotations, no Spring context, no Docker. */
class RestaurantRetryBackoffTest {

    private static final Pattern PLACEHOLDER = Pattern.compile("^\\$\\{[^:}]+:([^}]*)}$");

    private static String defaultOf(String expression) {
        Matcher m = PLACEHOLDER.matcher(expression);
        assertThat(m.matches()).as("expression %s is a ${key:default} placeholder", expression).isTrue();
        return m.group(1);
    }

    private static RetryableTopic retryable(Class<?> listener) {
        List<RetryableTopic> found = new ArrayList<>();
        for (Method method : listener.getDeclaredMethods()) {
            RetryableTopic annotation = method.getAnnotation(RetryableTopic.class);
            if (annotation != null) {
                found.add(annotation);
            }
        }
        assertThat(found).as("@RetryableTopic methods on %s", listener.getSimpleName()).hasSize(1);
        return found.get(0);
    }

    @ParameterizedTest
    @ValueSource(classes = {OrderEventsListener.class, DeliveryEventsListener.class})
    void productionDefaultsAreFourAttemptsWithOneFiveTwentyFiveSecondDelays(Class<?> listener) {
        RetryableTopic rt = retryable(listener);

        assertThat(Integer.parseInt(defaultOf(rt.attempts()))).isEqualTo(4);
        long delay = Long.parseLong(defaultOf(rt.backoff().delayExpression()));
        long multiplier = Long.parseLong(defaultOf(rt.backoff().multiplierExpression()));
        long max = Long.parseLong(defaultOf(rt.backoff().maxDelayExpression()));
        assertThat(delay).isEqualTo(1000);
        assertThat(multiplier).isEqualTo(5);
        assertThat(max).isEqualTo(30000);

        List<Long> delays = new ArrayList<>();
        long current = delay;
        for (int hop = 0; hop < 3; hop++) {
            delays.add(Math.min(current, max));
            current = current * multiplier;
        }
        assertThat(delays).containsExactly(1000L, 5000L, 25000L);
    }

    @ParameterizedTest
    @ValueSource(classes = {OrderEventsListener.class, DeliveryEventsListener.class})
    void propertyKeysAreTheDocumentedOnes(Class<?> listener) {
        RetryableTopic rt = retryable(listener);
        assertThat(rt.attempts()).startsWith("${app.kafka.retry.attempts:");
        assertThat(rt.backoff().delayExpression()).startsWith("${app.kafka.retry.delay-ms:");
        assertThat(rt.backoff().multiplierExpression()).startsWith("${app.kafka.retry.multiplier:");
        assertThat(rt.backoff().maxDelayExpression()).startsWith("${app.kafka.retry.max-delay-ms:");
    }

    @ParameterizedTest
    @ValueSource(classes = {OrderEventsListener.class, DeliveryEventsListener.class})
    void suffixesAutoCreateAndExclusions(Class<?> listener) {
        RetryableTopic rt = retryable(listener);

        assertThat(rt.retryTopicSuffix()).isEqualTo(".restaurant-service.retry");
        assertThat(rt.dltTopicSuffix()).isEqualTo(".restaurant-service.dlt");
        assertThat(rt.autoCreateTopics()).isEqualTo("false");
        assertThat(rt.topicSuffixingStrategy()).isEqualTo(TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE);
        assertThat(rt.dltStrategy()).isEqualTo(DltStrategy.FAIL_ON_ERROR);
        assertThat(rt.exclude()).contains(InvalidEventException.class);
    }

    @Test
    void suffixesProduceTheTopicNamesInTopicsConstants() {
        RetryableTopic order = retryable(OrderEventsListener.class);
        RetryableTopic delivery = retryable(DeliveryEventsListener.class);
        for (int i = 0; i < Topics.RETRY_HOPS; i++) {
            assertThat(Topics.ORDER_EVENTS + order.retryTopicSuffix() + "-" + i)
                    .isEqualTo(Topics.retry(Topics.ORDER_EVENTS, Topics.RESTAURANT_SERVICE, i));
            assertThat(Topics.DELIVERY_EVENTS + delivery.retryTopicSuffix() + "-" + i)
                    .isEqualTo(Topics.retry(Topics.DELIVERY_EVENTS, Topics.RESTAURANT_SERVICE, i));
        }
        assertThat(Topics.ORDER_EVENTS + order.dltTopicSuffix())
                .isEqualTo(Topics.dlt(Topics.ORDER_EVENTS, Topics.RESTAURANT_SERVICE));
        assertThat(Topics.DELIVERY_EVENTS + delivery.dltTopicSuffix())
                .isEqualTo(Topics.dlt(Topics.DELIVERY_EVENTS, Topics.RESTAURANT_SERVICE));
    }

    @ParameterizedTest
    @ValueSource(classes = {OrderEventsListener.class, DeliveryEventsListener.class})
    void everyListenerHasExactlyOneDltHandler(Class<?> listener) {
        int count = 0;
        for (Method method : listener.getDeclaredMethods()) {
            if (method.isAnnotationPresent(DltHandler.class)) {
                count++;
            }
        }
        assertThat(count).isEqualTo(1);
    }
}
