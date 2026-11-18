package com.example.fooddelivery.events;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Event type names (equal to the spec event names) and their payload classes. */
public final class EventTypes {

    public static final String ORDER_PLACED = "OrderPlaced";
    public static final String ORDER_CANCELLED = "OrderCancelled";
    public static final String ORDER_ACCEPTED = "OrderAccepted";
    public static final String ORDER_REJECTED = "OrderRejected";
    public static final String ORDER_READY = "OrderReady";
    public static final String DRIVER_ASSIGNED = "DriverAssigned";
    public static final String DELIVERY_FAILED = "DeliveryFailed";
    public static final String ORDER_PICKED_UP = "OrderPickedUp";
    public static final String ORDER_DELIVERED = "OrderDelivered";

    private static final Map<String, Class<? extends EventPayload>> PAYLOAD_CLASSES;

    static {
        Map<String, Class<? extends EventPayload>> m = new LinkedHashMap<>();
        m.put(ORDER_PLACED, OrderPlaced.class);
        m.put(ORDER_CANCELLED, OrderCancelled.class);
        m.put(ORDER_ACCEPTED, OrderAccepted.class);
        m.put(ORDER_REJECTED, OrderRejected.class);
        m.put(ORDER_READY, OrderReady.class);
        m.put(DRIVER_ASSIGNED, DriverAssigned.class);
        m.put(DELIVERY_FAILED, DeliveryFailed.class);
        m.put(ORDER_PICKED_UP, OrderPickedUp.class);
        m.put(ORDER_DELIVERED, OrderDelivered.class);
        PAYLOAD_CLASSES = Collections.unmodifiableMap(m);
    }

    private EventTypes() {
    }

    /** All event types mapped to their payload class, in catalogue order. */
    public static Map<String, Class<? extends EventPayload>> all() {
        return PAYLOAD_CLASSES;
    }

    /** Payload class for the type, or null if the type is unknown. */
    public static Class<? extends EventPayload> payloadClass(String eventType) {
        return eventType == null ? null : PAYLOAD_CLASSES.get(eventType);
    }

    /** Event type name for a payload instance. */
    public static String of(EventPayload payload) {
        for (Map.Entry<String, Class<? extends EventPayload>> e : PAYLOAD_CLASSES.entrySet()) {
            if (e.getValue().isInstance(payload)) {
                return e.getKey();
            }
        }
        throw new IllegalArgumentException("Unknown payload class " + payload.getClass().getName());
    }
}
