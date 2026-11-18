package com.example.fooddelivery.order;

import java.time.Clock;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/** Replaces the production clock with {@link TestClock}. */
@TestConfiguration
public class TestClockConfig {

    @Bean
    @Primary
    Clock testClock() {
        return TestClock.INSTANCE;
    }
}
