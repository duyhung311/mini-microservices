package com.example.fooddelivery.delivery.support;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

/**
 * Test-only helper, registered through META-INF/spring.factories so it sees every Spring test context.
 *
 * <p>All integration tests share one JVM and one Kafka group (delivery-service). Spring caches the contexts of
 * earlier test classes and their listener containers keep consuming, so a record may be handled by a cached
 * context that has no test hook. Classes that stub beans (retry tests) call {@link #stopOthers} to stop the
 * listener containers of every other live context and {@link #start} afterwards to restore them.
 */
public final class LiveContexts implements ApplicationContextInitializer<ConfigurableApplicationContext> {

    private static final List<ConfigurableApplicationContext> LIVE = new CopyOnWriteArrayList<>();

    @Override
    public void initialize(ConfigurableApplicationContext context) {
        LIVE.add(context);
        context.addApplicationListener((ApplicationListener<ContextClosedEvent>) event -> {
            if (event.getApplicationContext() == context) {
                LIVE.remove(context);
            }
        });
    }

    /** Stops the Kafka listener containers of all live contexts except {@code mine}; returns what was stopped. */
    public static List<KafkaListenerEndpointRegistry> stopOthers(ApplicationContext mine) {
        List<KafkaListenerEndpointRegistry> stopped = new ArrayList<>();
        for (ConfigurableApplicationContext other : LIVE) {
            if (other == mine || !other.isActive()) {
                continue;
            }
            ObjectProvider<KafkaListenerEndpointRegistry> provider =
                    other.getBeanProvider(KafkaListenerEndpointRegistry.class);
            KafkaListenerEndpointRegistry registry = provider.getIfAvailable();
            if (registry != null && registry.isRunning()) {
                registry.stop();
                stopped.add(registry);
            }
        }
        return stopped;
    }

    public static void start(List<KafkaListenerEndpointRegistry> registries) {
        for (KafkaListenerEndpointRegistry registry : registries) {
            registry.start();
        }
    }
}
