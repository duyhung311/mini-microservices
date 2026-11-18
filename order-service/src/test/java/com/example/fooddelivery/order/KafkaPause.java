package com.example.fooddelivery.order;

import org.testcontainers.DockerClientFactory;

/** Pauses and unpauses the shared Kafka container (the mapped port stays the same, unlike stop/start). */
final class KafkaPause {

    private KafkaPause() {
    }

    static void pause() {
        DockerClientFactory.instance().client().pauseContainerCmd(AbstractOrderIT.KAFKA.getContainerId()).exec();
    }

    /** Never throws, so it is safe in finally blocks. */
    static void unpauseQuietly() {
        try {
            DockerClientFactory.instance().client().unpauseContainerCmd(AbstractOrderIT.KAFKA.getContainerId())
                    .exec();
        } catch (RuntimeException e) {
            // not paused
        }
    }
}
