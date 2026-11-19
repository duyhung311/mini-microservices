# 0004. Per-group retry and DLT topic naming

## Status
Accepted (open question G, confirmed by the user).

## Context
The spec mentions `*.events.retry` and `*.events.dlt`. Every main topic has two consumer groups. If both groups shared one retry topic, a record that failed in one group would be read again by the other group, which already processed it.

## Decision
- Each (source topic, consumer group) pair has its own topics:
  - `<src>.events.<group>.retry-0`, `.retry-1`, `.retry-2` (3 partitions each)
  - `<src>.events.<group>.dlt` (1 partition)
- That is 6 pairs, so 18 retry topics and 6 DLTs. With the 3 main topics, 27 topics in total.
- Listeners use `@RetryableTopic` with 4 attempts and delays of 1 s, 5 s and 25 s (delay 1000 ms, multiplier 5, max 30000 ms). `autoCreateTopics=false`, so `kafka-init` creates them all.
- `InvalidEventException` is excluded from retries: a poison message goes to the DLT with zero hops.
- Each listener has a `@DltHandler` that logs `ERROR ALERT dlt topic=... key=... eventId=...` and increments `kafka.dlt.records`.
- The exact 1 s / 5 s / 30 s from the spec is not possible with the annotation (open question H). Exponential back-off gives 1 s / 5 s / 25 s. The user accepted this.

## Consequences
- A failure in one group never replays in the other.
- The topic list is long. `Topics.all()` in `common-events` is the single source, and `TopicsTest` and `check-topics.sh` check it.
- If a retry topic name differs from the pre-created list, Spring Kafka only warns. A `*RetryDltIT` asserts the subscribed topics equal the constants.
- Retry topics break per-key order. See ADR 0005.
