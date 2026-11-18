package com.example.fooddelivery.order;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;

/** A system clock shifted by a settable offset, shared by every context in the JVM that imports {@link TestClockConfig}. */
final class TestClock extends Clock {

    static final TestClock INSTANCE = new TestClock();

    private final AtomicReference<Duration> offset = new AtomicReference<>(Duration.ZERO);

    private TestClock() {
    }

    void advanceTo(Duration newOffset) {
        offset.set(newOffset);
    }

    void reset() {
        offset.set(Duration.ZERO);
    }

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return this;
    }

    @Override
    public Instant instant() {
        return Instant.now().plus(offset.get());
    }
}
