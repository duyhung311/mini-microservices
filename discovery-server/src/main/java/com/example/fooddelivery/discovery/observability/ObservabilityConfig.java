package com.example.fooddelivery.discovery.observability;

import io.micrometer.observation.ObservationPredicate;
import java.net.URI;
import org.springframework.http.client.observation.ClientRequestObservationContext;
import org.springframework.http.server.observation.ServerRequestObservationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Keeps traces readable: no scheduler ticks, no actuator scrapes, no Eureka chatter. */
@Configuration(proxyBeanMethods = false)
public class ObservabilityConfig {

    @Bean
    ObservationPredicate noiseFilter() {
        return (name, context) -> {
            if ("tasks.scheduled.execution".equals(name)) {
                return false;
            }
            if (context instanceof ServerRequestObservationContext server) {
                String uri = server.getCarrier().getRequestURI();
                if (uri != null && (uri.startsWith("/actuator") || uri.startsWith("/eureka/"))) {
                    return false;
                }
            }
            if (context instanceof ClientRequestObservationContext client && client.getCarrier() != null) {
                URI uri = client.getCarrier().getURI();
                if (uri != null && uri.getPath() != null && uri.getPath().contains("/eureka/")) {
                    return false;
                }
            }
            return true;
        };
    }
}
