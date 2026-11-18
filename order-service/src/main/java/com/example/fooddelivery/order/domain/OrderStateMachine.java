package com.example.fooddelivery.order.domain;

import java.util.List;

/**
 * Pure decision function for the order state machine (plan table). No Spring, no I/O.
 * <p>
 * {@code hasDriver}: orders.driver_id is not null. {@code readySeen}: the history contains
 * to_status=READY_FOR_PICKUP. Any (state, input) pair not listed as APPLY or PREMATURE is STALE, and
 * terminal states absorb every input.
 */
public final class OrderStateMachine {

    /** Side effects the caller has to carry out when a transition is applied. */
    public enum Effect {
        SET_DRIVER,
        EMIT_CANCELLED_NO_DRIVER,
        EMIT_CANCELLED_CUSTOMER_REQUEST,
        EMIT_CANCELLED_TIMEOUT
    }

    public sealed interface Decision permits Apply, Stale, Premature {
    }

    public record Apply(OrderStatus target, List<Effect> effects) implements Decision {
        public Apply {
            effects = List.copyOf(effects);
        }
    }

    public record Stale() implements Decision {
    }

    public record Premature() implements Decision {
    }

    private static final Stale STALE = new Stale();
    private static final Premature PREMATURE = new Premature();

    private OrderStateMachine() {
    }

    public static Decision decide(OrderStatus status, boolean hasDriver, boolean readySeen, OrderInput input) {
        if (status.isTerminal()) {
            return STALE;
        }
        switch (input) {
            case ORDER_ACCEPTED:
                return status == OrderStatus.PLACED ? apply(OrderStatus.ACCEPTED) : STALE;
            case ORDER_REJECTED:
                return status == OrderStatus.PLACED ? apply(OrderStatus.REJECTED) : STALE;
            case ORDER_READY:
                if (status == OrderStatus.PLACED) {
                    return PREMATURE;
                }
                if (status == OrderStatus.ACCEPTED || status == OrderStatus.DRIVER_ASSIGNED) {
                    return apply(OrderStatus.READY_FOR_PICKUP);
                }
                return STALE;
            case DRIVER_ASSIGNED:
                if (status == OrderStatus.PLACED) {
                    return PREMATURE;
                }
                if (status == OrderStatus.ACCEPTED || (status == OrderStatus.READY_FOR_PICKUP && !hasDriver)) {
                    return apply(OrderStatus.DRIVER_ASSIGNED, Effect.SET_DRIVER);
                }
                return STALE;
            case DELIVERY_FAILED:
                if (status == OrderStatus.PLACED) {
                    return PREMATURE;
                }
                if (status == OrderStatus.ACCEPTED || status == OrderStatus.DRIVER_ASSIGNED
                        || status == OrderStatus.READY_FOR_PICKUP) {
                    return apply(OrderStatus.CANCELLED, Effect.EMIT_CANCELLED_NO_DRIVER);
                }
                return STALE;
            case ORDER_PICKED_UP:
                if (status == OrderStatus.READY_FOR_PICKUP) {
                    return hasDriver ? apply(OrderStatus.OUT_FOR_DELIVERY) : PREMATURE;
                }
                if (status == OrderStatus.DRIVER_ASSIGNED) {
                    return readySeen ? apply(OrderStatus.OUT_FOR_DELIVERY) : PREMATURE;
                }
                if (status == OrderStatus.PLACED || status == OrderStatus.ACCEPTED) {
                    return PREMATURE;
                }
                return STALE;
            case ORDER_DELIVERED:
                if (status == OrderStatus.OUT_FOR_DELIVERY) {
                    return apply(OrderStatus.DELIVERED);
                }
                if (status == OrderStatus.PLACED || status == OrderStatus.ACCEPTED
                        || status == OrderStatus.DRIVER_ASSIGNED || status == OrderStatus.READY_FOR_PICKUP) {
                    return PREMATURE;
                }
                return STALE;
            case CUSTOMER_CANCEL:
                if (status == OrderStatus.PLACED || status == OrderStatus.ACCEPTED
                        || status == OrderStatus.DRIVER_ASSIGNED || status == OrderStatus.READY_FOR_PICKUP) {
                    return apply(OrderStatus.CANCELLED, Effect.EMIT_CANCELLED_CUSTOMER_REQUEST);
                }
                return STALE;
            case TIMEOUT:
                return status == OrderStatus.PLACED
                        ? apply(OrderStatus.CANCELLED, Effect.EMIT_CANCELLED_TIMEOUT) : STALE;
            default:
                return STALE;
        }
    }

    private static Apply apply(OrderStatus target, Effect... effects) {
        return new Apply(target, List.of(effects));
    }
}
