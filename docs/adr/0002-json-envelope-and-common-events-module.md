# 0002. JSON envelope and a shared common-events module

## Status
Accepted.

## Context
All 9 events must be readable by 2 to 3 services. The spec asks for an envelope with ids, type, version and correlation data. Avro and Schema Registry would add an extra service and tooling to a first Kafka project.

## Decision
- Events are JSON strings. Key and value are `String` on both sides.
- Every event is wrapped in the same envelope: `eventId`, `eventType`, `eventVersion` (always 1), `occurredAt`, `orderId`, `correlationId`, `causationId`, `payload`.
- The Kafka headers `eventType`, `eventVersion` and `correlationId` repeat the envelope values. A mismatch is treated as a poison message.
- The Maven module `common-events` holds the envelope, the 9 payload records, the enums, `EventCodec`, `Envelopes`, `EventHeaders` and `Topics`. It has no Spring config and no component-scanned beans.
- `causationId` is the `eventId` of the event that caused this one. It is `null` (but always present) for events that start from REST or from the scheduler.
- Consumers decode inside the listener. Any decode problem throws `InvalidEventException`.

## Consequences
- One place defines the wire format and the topic list. Tests, `kafka-init` and the services use the same names.
- Services share a library, so a change to `common-events` touches all three. That is a deliberate coupling for a learning project.
- JSON is easy to read in Kafka UI and with the console consumer.
- No schema enforcement at the broker. Avro plus Schema Registry is listed under future work.
- `eventVersion` is kept so a v2 can be introduced later.
