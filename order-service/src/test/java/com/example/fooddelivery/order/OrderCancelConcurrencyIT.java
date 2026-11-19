package com.example.fooddelivery.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * F5: a cancel that races a concurrent writer of the same order row must answer 200 or 409, never 500.
 * The concurrent writer is a real transaction that updates (and so bumps the version of) the row and commits only
 * after the cancel request is in flight.
 */
class OrderCancelConcurrencyIT extends AbstractOrderIT {

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private PlatformTransactionManager txManager;

    @Test
    void cancelRacingAConcurrentVersionBumpNeverReturns500() throws Exception {
        UUID orderId = UUID.fromString(placeOrder().get("orderId").asText());
        awaitPlaced(orderId);

        CountDownLatch rowLocked = new CountDownLatch(1);
        CountDownLatch releaseWriter = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<?> writer = pool.submit(() -> new TransactionTemplate(txManager).executeWithoutResult(tx -> {
                jdbc.update("update orders set version = version + 1, updated_at = now() where id = ?", orderId);
                rowLocked.countDown();
                try {
                    releaseWriter.await(20, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
            assertThat(rowLocked.await(10, TimeUnit.SECONDS)).isTrue();

            Future<ResponseEntity<JsonNode>> cancel = pool.submit(() -> post("/orders/" + orderId + "/cancel"));
            Thread.sleep(1500); // let the cancel read the order and reach its write while the other tx holds the row
            releaseWriter.countDown();
            writer.get(20, TimeUnit.SECONDS);

            int code = cancel.get(30, TimeUnit.SECONDS).getStatusCode().value();
            assertThat(code).as("cancel status").isIn(200, 409);
        } finally {
            releaseWriter.countDown();
            pool.shutdownNow();
        }
        assertThat(status(orderId)).isEqualTo("CANCELLED");
    }
}
