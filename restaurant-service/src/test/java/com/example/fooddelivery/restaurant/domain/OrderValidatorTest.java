package com.example.fooddelivery.restaurant.domain;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fooddelivery.events.RejectReason;
import com.example.fooddelivery.restaurant.domain.OrderValidator.RestaurantSnapshot;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Pure decision logic. Criterion 7 (CLOSED, ITEM_UNAVAILABLE, AT_CAPACITY) at unit level. */
class OrderValidatorTest {

    private static final UUID RESTAURANT = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID AVAILABLE_ITEM = UUID.fromString("22222222-2222-2222-2222-000000000001");
    private static final UUID OTHER_AVAILABLE_ITEM = UUID.fromString("22222222-2222-2222-2222-000000000002");
    private static final UUID UNAVAILABLE_ITEM = UUID.fromString("22222222-2222-2222-2222-000000000003");
    private static final UUID FOREIGN_ITEM = UUID.fromString("99999999-9999-9999-9999-000000000001");

    private static RestaurantSnapshot snapshot(boolean open, int max) {
        return new RestaurantSnapshot(RESTAURANT, open, max, Map.of(
                AVAILABLE_ITEM, true,
                OTHER_AVAILABLE_ITEM, true,
                UNAVAILABLE_ITEM, false));
    }

    @Test
    void acceptsOpenRestaurantWithAvailableItemsBelowCapacity() {
        assertThat(OrderValidator.validate(snapshot(true, 5), List.of(AVAILABLE_ITEM), 0)).isEmpty();
    }

    @Test
    void acceptsSeveralAvailableItems() {
        assertThat(OrderValidator.validate(snapshot(true, 5), List.of(AVAILABLE_ITEM, OTHER_AVAILABLE_ITEM), 2))
                .isEmpty();
    }

    @Test
    void nullSnapshotGivesClosed() {
        assertThat(OrderValidator.validate(null, List.of(AVAILABLE_ITEM), 0))
                .isEqualTo(Optional.of(RejectReason.CLOSED));
    }

    @Test
    void closedRestaurantGivesClosed() {
        assertThat(OrderValidator.validate(snapshot(false, 5), List.of(AVAILABLE_ITEM), 0))
                .isEqualTo(Optional.of(RejectReason.CLOSED));
    }

    @Test
    void unavailableItemGivesItemUnavailable() {
        assertThat(OrderValidator.validate(snapshot(true, 5), List.of(UNAVAILABLE_ITEM), 0))
                .isEqualTo(Optional.of(RejectReason.ITEM_UNAVAILABLE));
    }

    @Test
    void foreignOrUnknownItemGivesItemUnavailable() {
        assertThat(OrderValidator.validate(snapshot(true, 5), List.of(FOREIGN_ITEM), 0))
                .isEqualTo(Optional.of(RejectReason.ITEM_UNAVAILABLE));
    }

    @Test
    void oneBadItemAmongGoodOnesGivesItemUnavailable() {
        assertThat(OrderValidator.validate(snapshot(true, 5),
                List.of(AVAILABLE_ITEM, UNAVAILABLE_ITEM, OTHER_AVAILABLE_ITEM), 0))
                .isEqualTo(Optional.of(RejectReason.ITEM_UNAVAILABLE));
    }

    @Test
    void atCapacityGivesAtCapacity() {
        assertThat(OrderValidator.validate(snapshot(true, 5), List.of(AVAILABLE_ITEM), 5))
                .isEqualTo(Optional.of(RejectReason.AT_CAPACITY));
    }

    @Test
    void aboveCapacityGivesAtCapacity() {
        assertThat(OrderValidator.validate(snapshot(true, 5), List.of(AVAILABLE_ITEM), 9))
                .isEqualTo(Optional.of(RejectReason.AT_CAPACITY));
    }

    @Test
    void maxZeroRejectsEverythingWithAtCapacity() {
        assertThat(OrderValidator.validate(snapshot(true, 0), List.of(AVAILABLE_ITEM), 0))
                .isEqualTo(Optional.of(RejectReason.AT_CAPACITY));
    }

    @Test
    void boundaryOneBelowMaxAccepts() {
        assertThat(OrderValidator.validate(snapshot(true, 5), List.of(AVAILABLE_ITEM), 4)).isEmpty();
    }

    @Test
    void boundaryEqualToMaxRejects() {
        assertThat(OrderValidator.validate(snapshot(true, 5), List.of(AVAILABLE_ITEM), 5))
                .isEqualTo(Optional.of(RejectReason.AT_CAPACITY));
    }

    @Test
    void closedBeatsUnavailableItem() {
        assertThat(OrderValidator.validate(snapshot(false, 5), List.of(UNAVAILABLE_ITEM), 0))
                .isEqualTo(Optional.of(RejectReason.CLOSED));
    }

    @Test
    void closedBeatsCapacity() {
        assertThat(OrderValidator.validate(snapshot(false, 5), List.of(AVAILABLE_ITEM), 5))
                .isEqualTo(Optional.of(RejectReason.CLOSED));
    }

    @Test
    void nullSnapshotBeatsEverythingElse() {
        assertThat(OrderValidator.validate(null, List.of(UNAVAILABLE_ITEM, FOREIGN_ITEM), 100))
                .isEqualTo(Optional.of(RejectReason.CLOSED));
    }

    @Test
    void unavailableItemBeatsCapacity() {
        assertThat(OrderValidator.validate(snapshot(true, 5), List.of(UNAVAILABLE_ITEM), 5))
                .isEqualTo(Optional.of(RejectReason.ITEM_UNAVAILABLE));
    }
}
