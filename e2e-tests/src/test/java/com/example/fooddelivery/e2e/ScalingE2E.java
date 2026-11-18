package com.example.fooddelivery.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/**
 * Criterion 25 (Compose part). Counts order.events owners, not group members: each instance runs
 * 2 listener containers, so the group has 6 members.
 */
class ScalingE2E extends E2ESupport {

    @Test
    void orderEventsPartitionsAreOwnedByThreeDistinctHosts() {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA);
        try (Admin admin = Admin.create(p)) {
            Awaitility.await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(2))
                    .untilAsserted(() -> {
                        ConsumerGroupDescription d = admin
                                .describeConsumerGroups(Collections.singleton("restaurant-service"))
                                .describedGroups().get("restaurant-service").get();
                        Map<Integer, String> ownerHost = new HashMap<>();
                        for (MemberDescription m : d.members()) {
                            for (TopicPartition tp : m.assignment().topicPartitions()) {
                                if ("order.events".equals(tp.topic())) {
                                    assertThat(ownerHost.put(tp.partition(), m.host()))
                                            .as("partition %s has one owner", tp.partition()).isNull();
                                }
                            }
                        }
                        assertThat(ownerHost.keySet()).containsExactlyInAnyOrder(0, 1, 2);
                        assertThat(ownerHost.values()).doesNotHaveDuplicates().hasSize(3);
                    });
        }
    }
}
