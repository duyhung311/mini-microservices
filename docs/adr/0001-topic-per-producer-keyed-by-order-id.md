# 0001. One topic per producer, keyed by orderId

## Status
Accepted.

## Context
Three services take part in one order saga. We need to decide how events are spread over topics and what the record key is. Kafka only guarantees order inside one partition of one topic.

## Decision
- Each service owns one topic and is its only producer: `order.events`, `restaurant.events`, `delivery.events`. Each has 3 partitions and replication factor 1 (3 and min ISR 2 in the prod-like Compose file).
- The record key is always the `orderId` (a string). The key equals the `orderId` field in the envelope.
- Consumers subscribe to the topics of the other services. Each service has its own consumer group (`order-service`, `restaurant-service`, `delivery-service`).

## Consequences
- All events of one order from one producer sit on one partition and arrive in order.
- Events from two different producers are on two different topics, so there is no order between them. See ADR 0005.
- A single-producer topic is easy to reason about: the topic name tells you who emitted the event.
- Broker auto-create is off. `kafka-init` creates the 27 topics up front, and `scripts/check-topics.sh` checks them.
- Scaling is limited by 3 partitions: at most 3 active instances per group on one topic.
