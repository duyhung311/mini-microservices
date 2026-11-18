package com.example.fooddelivery.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.delivery.messaging.OrderEventsListener;
import com.example.fooddelivery.delivery.messaging.RestaurantEventsListener;
import com.example.fooddelivery.events.InvalidEventException;
import com.example.fooddelivery.events.Topics;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;

/** Criterion 18: production retry defaults (4 attempts, 1000 / 5000 / 25000 ms) read from the annotation. */
class DeliveryRetryBackoffTest {

    private static final Pattern PLACEHOLDER = Pattern.compile("^\\$\\{([^:}]+):([^}]*)}$");

    private static RetryableTopic annotationOf(Class<?> listener) {
        for (Method m : listener.getDeclaredMethods()) {
            RetryableTopic a = m.getAnnotation(RetryableTopic.class);
            if (a != null) {
                return a;
            }
        }
        throw new AssertionError("No @RetryableTopic method on " + listener.getName());
    }

    private static String defaultOf(String expression, String expectedKey) {
        Matcher m = PLACEHOLDER.matcher(expression);
        assertThat(m.matches()).as("expression %s is ${key:default}", expression).isTrue();
        assertThat(m.group(1)).isEqualTo(expectedKey);
        return m.group(2);
    }

    private static Class<?> listener(String name) {
        return "order".equals(name) ? OrderEventsListener.class : RestaurantEventsListener.class;
    }

    @ParameterizedTest
    @ValueSource(strings = {"order", "restaurant"})
    void productionDefaultsAreFourAttemptsWithFiveTimesBackoff(String which) {
        RetryableTopic a = annotationOf(listener(which));
        int attempts = Integer.parseInt(defaultOf(a.attempts(), "app.kafka.retry.attempts"));
        long delay = Long.parseLong(defaultOf(a.backoff().delayExpression(), "app.kafka.retry.delay-ms"));
        double multiplier = Double.parseDouble(
                defaultOf(a.backoff().multiplierExpression(), "app.kafka.retry.multiplier"));
        long max = Long.parseLong(defaultOf(a.backoff().maxDelayExpression(), "app.kafka.retry.max-delay-ms"));

        assertThat(attempts).isEqualTo(4);
        assertThat(multiplier).isEqualTo(5.0);
        assertThat(max).isEqualTo(30000L);

        List<Long> delays = new ArrayList<>();
        double d = delay;
        for (int i = 0; i < attempts - 1; i++) {
            delays.add(Math.min((long) d, max));
            d = d * multiplier;
        }
        assertThat(delays).containsExactly(1000L, 5000L, 25000L);
    }

    @ParameterizedTest
    @ValueSource(strings = {"order", "restaurant"})
    void retryAndDltTopicNamesMatchTheTopicConstants(String which) {
        RetryableTopic a = annotationOf(listener(which));
        String source = "order".equals(which) ? Topics.ORDER_EVENTS : Topics.RESTAURANT_EVENTS;

        assertThat(a.topicSuffixingStrategy()).isEqualTo(TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE);
        assertThat(a.retryTopicSuffix()).isEqualTo(".delivery-service.retry");
        assertThat(a.dltTopicSuffix()).isEqualTo(".delivery-service.dlt");
        for (int i = 0; i < Topics.RETRY_HOPS; i++) {
            assertThat(source + a.retryTopicSuffix() + "-" + i)
                    .isEqualTo(Topics.retry(source, Topics.DELIVERY_SERVICE, i));
        }
        assertThat(source + a.dltTopicSuffix()).isEqualTo(Topics.dlt(source, Topics.DELIVERY_SERVICE));
    }

    @ParameterizedTest
    @ValueSource(strings = {"order", "restaurant"})
    void topicsAreNotAutoCreatedAndInvalidEventsAreNeverRetried(String which) {
        RetryableTopic a = annotationOf(listener(which));
        assertThat(a.autoCreateTopics()).isEqualTo("false");
        assertThat(a.exclude()).contains(InvalidEventException.class);
    }

    @Test
    void bothListenersUseTheSamePolicy() {
        RetryableTopic o = annotationOf(OrderEventsListener.class);
        RetryableTopic r = annotationOf(RestaurantEventsListener.class);
        assertThat(o.attempts()).isEqualTo(r.attempts());
        assertThat(o.backoff().delayExpression()).isEqualTo(r.backoff().delayExpression());
        assertThat(o.backoff().multiplierExpression()).isEqualTo(r.backoff().multiplierExpression());
        assertThat(o.backoff().maxDelayExpression()).isEqualTo(r.backoff().maxDelayExpression());
    }
}
