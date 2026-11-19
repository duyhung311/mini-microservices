package com.example.fooddelivery.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.fooddelivery.delivery.support.DeliveryIntegrationTestBase;
import com.example.fooddelivery.events.EventTypes;
import java.sql.Connection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/**
 * Fix loop 1 (F6), deterministic: transaction A (plain JDBC) assigns a driver but has not committed. PATCH OFFLINE
 * finds no active delivery (A's change is invisible), then blocks on the driver row. The test waits until Postgres
 * reports that wait, commits A, and expects the PATCH to notice the new assignment and fail the delivery.
 */
class DeliveryOfflineRaceIT extends DeliveryIntegrationTestBase {

    @Test
    void offlineWhileAssignmentCommitsFailsTheDelivery() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        jdbc.update("INSERT INTO deliveries (id, order_id, status, correlation_id, dropoff_address, created_at,"
                + " updated_at) VALUES (?, ?, 'PENDING', ?, 'x', now(), now())",
                UUID.randomUUID(), orderId, correlationId);

        try (Connection a = jdbc.getDataSource().getConnection()) {
            a.setAutoCommit(false);
            try (var st = a.createStatement()) {
                st.execute("SELECT * FROM drivers WHERE id = '" + DRIVER_1 + "' FOR UPDATE");
                st.executeUpdate("UPDATE drivers SET status = 'BUSY' WHERE id = '" + DRIVER_1 + "'");
                st.executeUpdate("UPDATE deliveries SET status = 'ASSIGNED', driver_id = '" + DRIVER_1
                        + "', updated_at = now() WHERE order_id = '" + orderId + "'");
            }

            CompletableFuture<Resp> patch = CompletableFuture.supplyAsync(
                    () -> patchDriverStatus(DRIVER_1, "OFFLINE"));

            // the PATCH transaction is blocked on the driver row lock held by A
            await().atMost(WAIT).pollInterval(POLL).until(() -> jdbc.queryForObject(
                    "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock'"
                            + " AND query ILIKE '%FROM drivers WHERE id%'", Long.class) > 0);
            assertThat(patch).isNotDone();
            a.commit();

            Resp r = patch.get();
            assertThat(r.status()).isEqualTo(200);
            assertThat(r.body().get("status").asText()).isEqualTo("OFFLINE");
        }

        assertThat(deliveryStatus(orderId)).isEqualTo("FAILED");
        assertThat(driverStatus(DRIVER_1)).isEqualTo("OFFLINE");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND event_type = ?",
                Long.class, orderId, EventTypes.DELIVERY_FAILED)).isEqualTo(1);
    }
}
