# 0003. Transactional outbox with an advisory-lock polling relay

## Status
Accepted. It replaced a direct `KafkaTemplate` publisher that was used on purpose in milestones M2 to M5.

## Context
A service must change its database and publish an event. Doing both is a dual write: the database commit can succeed and the Kafka send fail, or the other way round. Early milestones used `send().get()` inside the transaction to show the problem.

## Decision
- Every service has an `outbox` table. The business change and the outbox row are written in one database transaction (`OutboxEventPublisher`, which requires an active transaction).
- `OutboxRelay` is a `@Scheduled` poller (every 500 ms). In one transaction it:
  1. takes `pg_try_advisory_xact_lock(hashtext('<service>-outbox'))` and returns if it did not get it;
  2. selects up to 100 unpublished rows `ORDER BY id FOR UPDATE`;
  3. sends each row with key = `aggregate_id`, waits up to 10 s, sets `published_at`;
  4. stops at the first failure and commits the rows already done.
- `payload` is `jsonb`, so Postgres changes key order and spacing. The relay re-encodes the payload with `EventCodec` so the wire JSON is canonical. This is the same in all 3 services.
- Only the `outbox` packages and the Kafka config may use `KafkaTemplate`. `scripts/verify.sh` greps for it.
- Delivery is at-least-once. Consumers are idempotent (`processed_events`).

## Consequences
- With Kafka paused, `POST /orders` still returns 202 and the row waits. After unpause it is published (`OutboxCrashIT`).
- The advisory lock means only one relay per service runs at a time, even with several instances. That keeps events of one aggregate in id order. The cost is one active relay per service.
- Polling adds up to about 500 ms of latency. Debezium CDC would remove the polling (future work).
- A crash between `send` and the commit can publish a row twice. The consumers absorb that.
