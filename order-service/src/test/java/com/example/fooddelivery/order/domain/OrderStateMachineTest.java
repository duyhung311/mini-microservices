package com.example.fooddelivery.order.domain;

import static com.example.fooddelivery.order.domain.OrderStatus.ACCEPTED;
import static com.example.fooddelivery.order.domain.OrderStatus.CANCELLED;
import static com.example.fooddelivery.order.domain.OrderStatus.DELIVERED;
import static com.example.fooddelivery.order.domain.OrderStatus.DRIVER_ASSIGNED;
import static com.example.fooddelivery.order.domain.OrderStatus.OUT_FOR_DELIVERY;
import static com.example.fooddelivery.order.domain.OrderStatus.PLACED;
import static com.example.fooddelivery.order.domain.OrderStatus.READY_FOR_PICKUP;
import static com.example.fooddelivery.order.domain.OrderStatus.REJECTED;
import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.order.domain.OrderStateMachine.Apply;
import com.example.fooddelivery.order.domain.OrderStateMachine.Decision;
import com.example.fooddelivery.order.domain.OrderStateMachine.Effect;
import com.example.fooddelivery.order.domain.OrderStateMachine.Premature;
import com.example.fooddelivery.order.domain.OrderStateMachine.Stale;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Criterion 11: every (status, input, hasDriver, readySeen) combination is checked against an independent
 * encoding of the table in plan.md "Order state machine". Anything not listed there must be STALE.
 */
class OrderStateMachineTest {

    private static final Set<OrderStatus> TERMINAL = EnumSet.of(DELIVERED, REJECTED, CANCELLED);
    private static final Set<OrderStatus> CANCELLABLE =
            EnumSet.of(PLACED, ACCEPTED, DRIVER_ASSIGNED, READY_FOR_PICKUP);

    /** Expected outcome; target == null and premature == false means STALE. */
    private record Expected(OrderStatus target, List<Effect> effects, boolean prematureFlag) {
        static Expected apply(OrderStatus target, Effect... effects) {
            return new Expected(target, List.of(effects), false);
        }

        static Expected stale() {
            return new Expected(null, List.of(), false);
        }

        static Expected premature() {
            return new Expected(null, List.of(), true);
        }
    }

    /** The table from the plan, written independently of the implementation. */
    private static Expected expected(OrderStatus s, OrderInput in, boolean hasDriver, boolean readySeen) {
        if (TERMINAL.contains(s)) {
            return Expected.stale();
        }
        switch (in) {
            case ORDER_ACCEPTED:
                return s == PLACED ? Expected.apply(ACCEPTED) : Expected.stale();
            case ORDER_REJECTED:
                return s == PLACED ? Expected.apply(REJECTED) : Expected.stale();
            case ORDER_READY:
                if (s == PLACED) {
                    return Expected.premature();
                }
                if (s == ACCEPTED || s == DRIVER_ASSIGNED) {
                    return Expected.apply(READY_FOR_PICKUP);
                }
                return Expected.stale();
            case DRIVER_ASSIGNED:
                if (s == PLACED) {
                    return Expected.premature();
                }
                if (s == ACCEPTED) {
                    return Expected.apply(DRIVER_ASSIGNED, Effect.SET_DRIVER);
                }
                if (s == READY_FOR_PICKUP && !hasDriver) {
                    return Expected.apply(DRIVER_ASSIGNED, Effect.SET_DRIVER);
                }
                return Expected.stale();
            case DELIVERY_FAILED:
                if (s == PLACED) {
                    return Expected.premature();
                }
                if (s == ACCEPTED || s == DRIVER_ASSIGNED || s == READY_FOR_PICKUP) {
                    return Expected.apply(CANCELLED, Effect.EMIT_CANCELLED_NO_DRIVER);
                }
                return Expected.stale();
            case ORDER_PICKED_UP:
                if (s == READY_FOR_PICKUP) {
                    return hasDriver ? Expected.apply(OUT_FOR_DELIVERY) : Expected.premature();
                }
                if (s == DRIVER_ASSIGNED) {
                    return readySeen ? Expected.apply(OUT_FOR_DELIVERY) : Expected.premature();
                }
                if (s == PLACED || s == ACCEPTED) {
                    return Expected.premature();
                }
                return Expected.stale(); // OUT_FOR_DELIVERY: already picked up
            case ORDER_DELIVERED:
                if (s == OUT_FOR_DELIVERY) {
                    return Expected.apply(DELIVERED);
                }
                if (s == PLACED || s == ACCEPTED || s == DRIVER_ASSIGNED || s == READY_FOR_PICKUP) {
                    return Expected.premature();
                }
                return Expected.stale();
            case CUSTOMER_CANCEL:
                return CANCELLABLE.contains(s)
                        ? Expected.apply(CANCELLED, Effect.EMIT_CANCELLED_CUSTOMER_REQUEST)
                        : Expected.stale();
            case TIMEOUT:
                return s == PLACED ? Expected.apply(CANCELLED, Effect.EMIT_CANCELLED_TIMEOUT) : Expected.stale();
            default:
                throw new IllegalStateException("Unhandled input " + in);
        }
    }

    private static List<Arguments> combinations(OrderStatus[] statuses) {
        List<Arguments> all = new ArrayList<>();
        for (OrderStatus s : statuses) {
            for (OrderInput in : OrderInput.values()) {
                for (boolean hasDriver : new boolean[] {false, true}) {
                    for (boolean readySeen : new boolean[] {false, true}) {
                        all.add(Arguments.of(s, in, hasDriver, readySeen));
                    }
                }
            }
        }
        return all;
    }

    static Stream<Arguments> allCombinations() {
        return combinations(OrderStatus.values()).stream();
    }

    static Stream<Arguments> terminalCombinations() {
        return combinations(TERMINAL.toArray(new OrderStatus[0])).stream();
    }

    @ParameterizedTest(name = "{0} + {1} (hasDriver={2}, readySeen={3})")
    @MethodSource("allCombinations")
    void everyCombinationMatchesTheTable(OrderStatus s, OrderInput in, boolean hasDriver, boolean readySeen) {
        Expected exp = expected(s, in, hasDriver, readySeen);
        Decision actual = OrderStateMachine.decide(s, hasDriver, readySeen, in);

        if (exp.prematureFlag()) {
            assertThat(actual).isInstanceOf(Premature.class);
        } else if (exp.target() == null) {
            assertThat(actual).isInstanceOf(Stale.class);
        } else {
            assertThat(actual).isInstanceOf(Apply.class);
            Apply apply = (Apply) actual;
            assertThat(apply.target()).isEqualTo(exp.target());
            assertThat(apply.effects()).containsExactlyElementsOf(exp.effects());
        }
    }

    @ParameterizedTest(name = "terminal {0} absorbs {1} (hasDriver={2}, readySeen={3})")
    @MethodSource("terminalCombinations")
    void terminalStatesAbsorbEveryInput(OrderStatus s, OrderInput in, boolean hasDriver, boolean readySeen) {
        assertThat(OrderStateMachine.decide(s, hasDriver, readySeen, in)).isInstanceOf(Stale.class);
    }

    // ---- explicit, human-readable spot checks of the effects named in the criteria ----

    @Test
    void driverAssignedSetsDriverFromAccepted() {
        assertThat(OrderStateMachine.decide(ACCEPTED, false, false, OrderInput.DRIVER_ASSIGNED))
                .isEqualTo(new Apply(DRIVER_ASSIGNED, List.of(Effect.SET_DRIVER)));
    }

    @Test
    void driverAssignedAfterReadyGoesToDriverAssignedWhenNoDriverYet() {
        assertThat(OrderStateMachine.decide(READY_FOR_PICKUP, false, true, OrderInput.DRIVER_ASSIGNED))
                .isEqualTo(new Apply(DRIVER_ASSIGNED, List.of(Effect.SET_DRIVER)));
    }

    @Test
    void driverAssignedAfterReadyWithDriverIsStale() {
        assertThat(OrderStateMachine.decide(READY_FOR_PICKUP, true, true, OrderInput.DRIVER_ASSIGNED))
                .isInstanceOf(Stale.class);
    }

    @Test
    void deliveryFailedCancelsWithNoDriverEffect() {
        for (OrderStatus s : List.of(ACCEPTED, DRIVER_ASSIGNED, READY_FOR_PICKUP)) {
            assertThat(OrderStateMachine.decide(s, true, true, OrderInput.DELIVERY_FAILED))
                    .isEqualTo(new Apply(CANCELLED, List.of(Effect.EMIT_CANCELLED_NO_DRIVER)));
        }
    }

    @Test
    void customerCancelEmitsCustomerRequest() {
        for (OrderStatus s : CANCELLABLE) {
            assertThat(OrderStateMachine.decide(s, false, false, OrderInput.CUSTOMER_CANCEL))
                    .isEqualTo(new Apply(CANCELLED, List.of(Effect.EMIT_CANCELLED_CUSTOMER_REQUEST)));
        }
        assertThat(OrderStateMachine.decide(OUT_FOR_DELIVERY, true, true, OrderInput.CUSTOMER_CANCEL))
                .isInstanceOf(Stale.class);
    }

    @Test
    void timeoutFromPlacedEmitsTimeout() {
        assertThat(OrderStateMachine.decide(PLACED, false, false, OrderInput.TIMEOUT))
                .isEqualTo(new Apply(CANCELLED, List.of(Effect.EMIT_CANCELLED_TIMEOUT)));
        assertThat(OrderStateMachine.decide(ACCEPTED, false, false, OrderInput.TIMEOUT)).isInstanceOf(Stale.class);
    }

    @Test
    void rejectedFromPlacedHasNoCancelEffect() {
        Decision d = OrderStateMachine.decide(PLACED, false, false, OrderInput.ORDER_REJECTED);
        assertThat(d).isInstanceOf(Apply.class);
        assertThat(((Apply) d).target()).isEqualTo(REJECTED);
        assertThat(((Apply) d).effects()).isEmpty();
    }

    @Test
    void prematurePickedUpCases() {
        assertThat(OrderStateMachine.decide(DRIVER_ASSIGNED, true, false, OrderInput.ORDER_PICKED_UP))
                .isInstanceOf(Premature.class);
        assertThat(OrderStateMachine.decide(DRIVER_ASSIGNED, true, true, OrderInput.ORDER_PICKED_UP))
                .isEqualTo(new Apply(OUT_FOR_DELIVERY, List.of()));
        assertThat(OrderStateMachine.decide(READY_FOR_PICKUP, false, true, OrderInput.ORDER_PICKED_UP))
                .isInstanceOf(Premature.class);
        assertThat(OrderStateMachine.decide(READY_FOR_PICKUP, true, true, OrderInput.ORDER_PICKED_UP))
                .isEqualTo(new Apply(OUT_FOR_DELIVERY, List.of()));
    }
}
