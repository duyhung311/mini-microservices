package com.example.fooddelivery.events.tracing;

import io.micrometer.observation.Observation;
import io.micrometer.observation.contextpropagation.ObservationThreadLocalAccessor;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.MDC;

/**
 * Glue between the saga's correlationId and the distributed trace (M10).
 *
 * <p>Rule: one saga is one trace, and traceId == correlationId without dashes. POST /orders derives the correlationId
 * from the current traceId. Every outbox row stores a W3C traceparent inside that trace, and the relay publishes
 * from it, so the async hop and all downstream consumers stay in the same trace. Plain helper, no Spring beans.
 */
public final class TraceLinks {

    public static final String CORRELATION_KEY = "correlationId";

    private static final Pattern TRACEPARENT =
            Pattern.compile("^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$");

    private TraceLinks() {
    }

    /** The 32 hex characters of a UUID, i.e. the traceId a correlationId maps to. */
    public static String traceIdOf(UUID correlationId) {
        return correlationId.toString().replace("-", "");
    }

    /** Formats a 32 hex traceId as a UUID (dashes 8-4-4-4-12), or returns null if it is not a valid traceId. */
    public static UUID uuidOfTraceId(String traceId) {
        if (traceId == null || !traceId.matches("[0-9a-f]{32}") || traceId.matches("0{32}")) {
            return null;
        }
        return UUID.fromString(traceId.substring(0, 8) + "-" + traceId.substring(8, 12) + "-"
                + traceId.substring(12, 16) + "-" + traceId.substring(16, 20) + "-" + traceId.substring(20));
    }

    /** correlationId for a new saga: the current traceId as UUID, or a random UUID when no trace is active. */
    public static UUID currentCorrelationIdOrRandom(Tracer tracer) {
        if (tracer != null) {
            Span span = tracer.currentSpan();
            if (span != null && span.context() != null) {
                UUID fromTrace = uuidOfTraceId(span.context().traceId());
                if (fromTrace != null) {
                    return fromTrace;
                }
            }
        }
        return UUID.randomUUID();
    }

    /**
     * Returns the traceparent to store with an outbox row, inside the saga trace (traceId == correlationId).
     * If the current span already belongs to that trace it is the parent. Otherwise (REST calls and scheduler jobs
     * that start a different trace) a short "outbox enqueue" span is created in the saga trace, as a child of a
     * synthetic parent id, and its id is returned. The foreign traceId is kept as the tag {@code origin.traceId}.
     * Returns null when tracing is unavailable.
     */
    public static String capture(Tracer tracer, UUID correlationId, String eventType) {
        if (tracer == null) {
            return null;
        }
        String sagaTraceId = traceIdOf(correlationId);
        tagCurrentObservation(correlationId);
        Span current = tracer.currentSpan();
        if (current != null && current.context() != null && sagaTraceId.equals(current.context().traceId())) {
            current.tag(CORRELATION_KEY, correlationId.toString());
            return format(current.context());
        }
        TraceContext parent = tracer.traceContextBuilder().traceId(sagaTraceId).spanId(randomSpanId())
                .sampled(true).build();
        Span.Builder builder = tracer.spanBuilder().setParent(parent).name("outbox enqueue")
                .tag(CORRELATION_KEY, correlationId.toString()).tag("event.type", eventType);
        if (current != null && current.context() != null && !current.context().traceId().isEmpty()) {
            builder.tag("origin.traceId", current.context().traceId());
        }
        Span span = builder.start();
        try {
            return format(span.context());
        } finally {
            span.end();
        }
    }

    /** Starts a span as child of the stored traceparent. Returns null if there is none or it is invalid. */
    public static Span startChildOf(Tracer tracer, String traceparent, String name) {
        if (tracer == null || traceparent == null) {
            return null;
        }
        Matcher m = TRACEPARENT.matcher(traceparent);
        if (!m.matches()) {
            return null;
        }
        boolean sampled = (Integer.parseInt(m.group(3), 16) & 1) == 1;
        TraceContext parent = tracer.traceContextBuilder().traceId(m.group(1)).spanId(m.group(2))
                .sampled(sampled).build();
        return tracer.spanBuilder().setParent(parent).name(name).start();
    }

    /**
     * Runs {@code body} inside a span that is a child of the stored traceparent, so spans created by it (Kafka
     * producer observation) join the saga trace. Without a usable traceparent it just runs the body.
     */
    public static void inChildSpan(Tracer tracer, String traceparent, String name, UUID correlationId,
            String eventType, Object outboxId, ThrowingRunnable body) throws Exception {
        Span span = startChildOf(tracer, traceparent, name);
        if (span == null) {
            body.run();
            return;
        }
        span.tag(CORRELATION_KEY, String.valueOf(correlationId)).tag("event.type", eventType)
                .tag("outbox.id", String.valueOf(outboxId));
        try (Tracer.SpanInScope ignored = tracer.withSpan(span)) {
            body.run();
        } catch (Exception e) {
            span.error(e);
            throw e;
        } finally {
            span.end();
        }
    }

    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Exception;
    }

    /** W3C traceparent for a context, or null when the context is not a valid trace (for example a no-op tracer). */
    public static String format(TraceContext ctx) {
        if (ctx == null || ctx.traceId() == null || !ctx.traceId().matches("[0-9a-f]{32}")
                || ctx.spanId() == null || !ctx.spanId().matches("[0-9a-f]{16}")) {
            return null;
        }
        return "00-" + ctx.traceId() + "-" + ctx.spanId() + "-" + (Boolean.FALSE.equals(ctx.sampled()) ? "00" : "01");
    }

    /** Adds the correlationId as a tag to the current observation's span and to the MDC, until closed. */
    public static Binding bindCorrelation(UUID correlationId) {
        String value = String.valueOf(correlationId);
        tagCurrentObservation(correlationId);
        String previous = MDC.get(CORRELATION_KEY);
        MDC.put(CORRELATION_KEY, value);
        return () -> {
            if (previous == null) {
                MDC.remove(CORRELATION_KEY);
            } else {
                MDC.put(CORRELATION_KEY, previous);
            }
        };
    }

    /** Tags the current observation (HTTP request, Kafka listener) so its span carries the correlationId. */
    public static void tagCurrentObservation(UUID correlationId) {
        Observation observation = ObservationThreadLocalAccessor.getInstance().getValue();
        if (observation != null) {
            observation.highCardinalityKeyValue(CORRELATION_KEY, String.valueOf(correlationId));
        }
    }

    /** Closeable that does not throw. */
    @FunctionalInterface
    public interface Binding extends AutoCloseable {
        @Override
        void close();
    }

    private static String randomSpanId() {
        long id;
        do {
            id = ThreadLocalRandom.current().nextLong();
        } while (id == 0);
        return String.format("%016x", id);
    }
}
