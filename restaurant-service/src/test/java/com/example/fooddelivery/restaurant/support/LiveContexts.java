package com.example.fooddelivery.restaurant.support;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

/**
 * Test fixture, registered through src/test/resources/META-INF/spring.factories. Spring caches the application
 * contexts of earlier test classes and their Kafka listeners stay alive in the same consumer group, so they would
 * compete with the context under test for records (and apply their own retry delays). This class tracks every started
 * context so a test class can silence the listeners of all other contexts for its duration and restore them after.
 */
public class LiveContexts implements ApplicationListener<ApplicationReadyEvent> {

    private static final List<ConfigurableApplicationContext> CONTEXTS = new CopyOnWriteArrayList<>();

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        CONTEXTS.add(event.getApplicationContext());
    }

    /** Stops the Kafka listener containers of every other live context and returns what was stopped. */
    public static List<KafkaListenerEndpointRegistry> stopListenersExcept(ApplicationContext mine) {
        List<KafkaListenerEndpointRegistry> stopped = new ArrayList<>();
        for (ConfigurableApplicationContext ctx : CONTEXTS) {
            if (ctx == mine || !ctx.isActive()) {
                continue;
            }
            KafkaListenerEndpointRegistry registry = ctx.getBeanProvider(KafkaListenerEndpointRegistry.class)
                    .getIfAvailable();
            if (registry != null && registry.isRunning()) {
                registry.stop();
                stopped.add(registry);
            }
        }
        return stopped;
    }

    public static void restart(List<KafkaListenerEndpointRegistry> registries) {
        for (KafkaListenerEndpointRegistry registry : registries) {
            registry.start();
        }
    }
}
