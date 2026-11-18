package com.example.fooddelivery.gateway.observability;

import io.micrometer.observation.ObservationPredicate;
import org.springframework.http.server.reactive.observation.ServerRequestObservationContext;
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
                String path = server.getCarrier().getPath().value();
                if (path.startsWith("/actuator")) {
                    return false;
                }
            }
            return true;
        };
    }
}
