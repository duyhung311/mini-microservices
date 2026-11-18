package com.example.fooddelivery.events;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Topic names and partition counts. The single source of truth for kafka-init, scripts and tests. */
public final class Topics {

    public static final String ORDER_EVENTS = "order.events";
    public static final String RESTAURANT_EVENTS = "restaurant.events";
    public static final String DELIVERY_EVENTS = "delivery.events";

    public static final String ORDER_SERVICE = "order-service";
    public static final String RESTAURANT_SERVICE = "restaurant-service";
    public static final String DELIVERY_SERVICE = "delivery-service";

    public static final int MAIN_PARTITIONS = 3;
    public static final int RETRY_PARTITIONS = 3;
    public static final int DLT_PARTITIONS = 1;
    public static final int RETRY_HOPS = 3;

    /** Topic name and partition count. */
    public record TopicSpec(String name, int partitions) {
    }

    /** One consumer (source topic, consumer group) pair. */
    public record Subscription(String topic, String group) {
    }

    private static final List<Subscription> SUBSCRIPTIONS = List.of(
            new Subscription(ORDER_EVENTS, RESTAURANT_SERVICE),
            new Subscription(ORDER_EVENTS, DELIVERY_SERVICE),
            new Subscription(RESTAURANT_EVENTS, ORDER_SERVICE),
            new Subscription(RESTAURANT_EVENTS, DELIVERY_SERVICE),
            new Subscription(DELIVERY_EVENTS, ORDER_SERVICE),
            new Subscription(DELIVERY_EVENTS, RESTAURANT_SERVICE));

    private static final List<TopicSpec> ALL = buildAll();

    private Topics() {
    }

    /** {@code <src>.events.<group>.retry-<index>}, index 0..2. */
    public static String retry(String sourceTopic, String group, int index) {
        return sourceTopic + "." + group + ".retry-" + index;
    }

    /** {@code <src>.events.<group>.dlt}. */
    public static String dlt(String sourceTopic, String group) {
        return sourceTopic + "." + group + ".dlt";
    }

    /** The 6 (source topic, consumer group) pairs. */
    public static List<Subscription> subscriptions() {
        return SUBSCRIPTIONS;
    }

    /** All 27 topics: 3 main, 18 retry, 6 DLT, in that order. */
    public static List<TopicSpec> all() {
        return ALL;
    }

    private static List<TopicSpec> buildAll() {
        List<TopicSpec> list = new ArrayList<>();
        list.add(new TopicSpec(ORDER_EVENTS, MAIN_PARTITIONS));
        list.add(new TopicSpec(RESTAURANT_EVENTS, MAIN_PARTITIONS));
        list.add(new TopicSpec(DELIVERY_EVENTS, MAIN_PARTITIONS));
        for (Subscription s : SUBSCRIPTIONS) {
            for (int i = 0; i < RETRY_HOPS; i++) {
                list.add(new TopicSpec(retry(s.topic(), s.group(), i), RETRY_PARTITIONS));
            }
        }
        for (Subscription s : SUBSCRIPTIONS) {
            list.add(new TopicSpec(dlt(s.topic(), s.group()), DLT_PARTITIONS));
        }
        return Collections.unmodifiableList(list);
    }
}
