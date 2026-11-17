# Food Delivery Order Pipeline — Spec

> A learning project: 3 Spring Boot microservices communicating via Kafka using event-driven choreography (saga pattern).

---

## 1. Goals

### Learning goals
- Event-driven choreography (no central orchestrator)
- Saga pattern with compensating events
- Partition keying for per-entity ordering
- Idempotent consumers and at-least-once delivery
- Transactional outbox pattern
- Retry topics and dead-letter topics (DLT)
- Consumer groups and horizontal scaling
- Schema design and versioning for events
- Integration testing with Testcontainers

### Non-goals (for v1)
- Real payments, real maps, or real routing
- Frontend UI (use REST calls / Postman / Kafka UI)
- Authentication and authorization
- Kubernetes deployment

---

## 2. Domain overview

A customer places an order. The restaurant accepts or rejects it, then prepares it. A driver is assigned, picks it up, and delivers it. The order status is updated as events flow back.

```
Customer ──REST──▶ Order Service ──▶ Kafka ──▶ Restaurant Service
                        ▲                              │
                        │                              ▼
                        └──────── Kafka ◀──── Delivery Service
```

---

## 3. Services

### 3.1 Order Service
**Owns:** orders, order status, order history.

**Responsibilities**
- Expose REST API to create, view, and cancel orders
- Publish `OrderPlaced` and `OrderCancelled`
- Consume restaurant and delivery events to update order status
- Act as the source of truth for order state

### 3.2 Restaurant Service
**Owns:** restaurants, menus, kitchen tickets.

**Responsibilities**
- Consume `OrderPlaced`, validate items and restaurant open/capacity
- Publish `OrderAccepted` or `OrderRejected`
- Simulate kitchen prep, publish `OrderReady`
- Consume `OrderCancelled` to cancel a kitchen ticket (compensation)
- Consume `DeliveryFailed` to mark the ticket as abandoned

### 3.3 Delivery Service
**Owns:** drivers, delivery assignments.

**Responsibilities**
- Consume `OrderAccepted`, try to assign an available driver
- Publish `DriverAssigned` or `DeliveryFailed` (no driver available)
- Simulate pickup and delivery, publish `OrderPickedUp`, `OrderDelivered`
- Consume `OrderCancelled` to release the driver (compensation)

---

## 4. Kafka topics

| Topic | Producer | Consumers | Key | Partitions | Notes |
|---|---|---|---|---|---|
| `order.events` | Order | Restaurant, Delivery | `orderId` | 3 | OrderPlaced, OrderCancelled |
| `restaurant.events` | Restaurant | Order, Delivery | `orderId` | 3 | OrderAccepted, OrderRejected, OrderReady |
| `delivery.events` | Delivery | Order, Restaurant | `orderId` | 3 | DriverAssigned, DeliveryFailed, OrderPickedUp, OrderDelivered |
| `*.events.retry` | Spring Kafka | same group | `orderId` | 3 | Non-blocking retries |
| `*.events.dlt` | Spring Kafka | ops / manual | `orderId` | 1 | Poison messages |

**Decisions**
- **One topic per producing service** (not per event type). This keeps all events for an order in one partition per topic, preserving order. The event type goes in a header and the envelope.
- **Key = `orderId`** everywhere, so all events for one order land on the same partition and are processed in order.
- Replication factor: 1 locally, 3 in a "prod-like" profile.

---

## 5. Event envelope

Every event shares a common envelope:

```json
{
  "eventId": "uuid",
  "eventType": "OrderPlaced",
  "eventVersion": 1,
  "occurredAt": "2026-11-17T12:00:00Z",
  "orderId": "uuid",
  "correlationId": "uuid",
  "causationId": "uuid-of-triggering-event",
  "payload": { }
}
```

- `eventId` is used for idempotency (dedup on the consumer side).
- `correlationId` stays the same across the whole saga, for tracing.
- `causationId` points to the event that caused this one.

**Kafka headers:** `eventType`, `eventVersion`, `correlationId`.

---

## 6. Event catalogue

### order.events
**OrderPlaced**
```json
{
  "customerId": "uuid",
  "restaurantId": "uuid",
  "items": [{ "menuItemId": "uuid", "quantity": 2, "unitPrice": 12.50 }],
  "totalAmount": 25.00,
  "deliveryAddress": "string"
}
```

**OrderCancelled**
```json
{ "reason": "CUSTOMER_REQUEST | RESTAURANT_REJECTED | NO_DRIVER | TIMEOUT" }
```

### restaurant.events
**OrderAccepted**: `{ "restaurantId": "uuid", "estimatedPrepMinutes": 15, "pickupAddress": "string" }`
**OrderRejected**: `{ "restaurantId": "uuid", "reason": "CLOSED | ITEM_UNAVAILABLE | AT_CAPACITY" }`
**OrderReady**: `{ "restaurantId": "uuid", "readyAt": "timestamp" }`

### delivery.events
**DriverAssigned**: `{ "driverId": "uuid", "driverName": "string", "etaMinutes": 20 }`
**DeliveryFailed**: `{ "reason": "NO_DRIVER_AVAILABLE | DRIVER_CANCELLED" }`
**OrderPickedUp**: `{ "driverId": "uuid", "pickedUpAt": "timestamp" }`
**OrderDelivered**: `{ "driverId": "uuid", "deliveredAt": "timestamp" }`

---

## 7. Saga flows

### 7.1 Happy path
```
1. Customer → POST /orders
2. Order       → OrderPlaced          (status: PLACED)
3. Restaurant  → OrderAccepted        (status: ACCEPTED)
4. Delivery    → DriverAssigned       (status: DRIVER_ASSIGNED)
5. Restaurant  → OrderReady           (status: READY_FOR_PICKUP)
6. Delivery    → OrderPickedUp        (status: OUT_FOR_DELIVERY)
7. Delivery    → OrderDelivered       (status: DELIVERED)
```

Note: steps 4 and 5 can happen in either order. Order Service must handle both.

### 7.2 Restaurant rejects
```
Order       → OrderPlaced
Restaurant  → OrderRejected
Order       → status: REJECTED (terminal, no compensation needed)
```

### 7.3 No driver available (compensation)
```
Order       → OrderPlaced
Restaurant  → OrderAccepted
Delivery    → DeliveryFailed(NO_DRIVER_AVAILABLE)
Order       → OrderCancelled(NO_DRIVER)       status: CANCELLED
Restaurant  → consumes OrderCancelled → cancels kitchen ticket
```

### 7.4 Customer cancels
- Allowed only while status is `PLACED`, `ACCEPTED`, or `DRIVER_ASSIGNED`.
- Order publishes `OrderCancelled(CUSTOMER_REQUEST)`.
- Restaurant cancels the ticket, and Delivery releases the driver.
- Rejected with `409 Conflict` once the order is `OUT_FOR_DELIVERY`.

### 7.5 Timeout (stretch)
- If an order stays in `PLACED` for more than N minutes, Order Service publishes `OrderCancelled(TIMEOUT)`.
- Implement with a scheduled job that scans stale orders.

---

## 8. Order state machine

```
PLACED ──▶ ACCEPTED ──▶ DRIVER_ASSIGNED ──▶ READY_FOR_PICKUP ──▶ OUT_FOR_DELIVERY ──▶ DELIVERED
  │           │   └──▶ READY_FOR_PICKUP ──▶ DRIVER_ASSIGNED ──┘
  │           │
  ▼           ▼
REJECTED   CANCELLED  (also from DRIVER_ASSIGNED / READY_FOR_PICKUP)
```

**Rules**
- Terminal states: `DELIVERED`, `REJECTED`, `CANCELLED`.
- Events that don't fit a valid transition are logged and ignored (not retried), e.g. `OrderReady` arriving after `CANCELLED`.
- Store every transition in an `order_status_history` table.

---

## 9. Data model (PostgreSQL, one DB per service)

### order-db
- `orders` (id, customer_id, restaurant_id, status, total_amount, delivery_address, driver_id, created_at, updated_at, version)
- `order_items` (id, order_id, menu_item_id, quantity, unit_price)
- `order_status_history` (id, order_id, from_status, to_status, event_id, changed_at)
- `outbox` (id, aggregate_id, event_type, payload jsonb, created_at, published_at)
- `processed_events` (event_id PK, processed_at)

### restaurant-db
- `restaurants` (id, name, address, is_open, max_concurrent_orders)
- `menu_items` (id, restaurant_id, name, price, available)
- `kitchen_tickets` (id, order_id, restaurant_id, status, created_at)
- `outbox`, `processed_events`

### delivery-db
- `drivers` (id, name, status: AVAILABLE | BUSY | OFFLINE)
- `deliveries` (id, order_id, driver_id, status, pickup_address, dropoff_address, created_at)
- `outbox`, `processed_events`

---

## 10. Reliability patterns

### 10.1 Transactional outbox
- A business change and its event row are written in the **same DB transaction**.
- A relay polls `outbox` where `published_at IS NULL`, publishes to Kafka, then sets `published_at`.
- v1: a simple `@Scheduled` poller. Stretch: Debezium CDC.

### 10.2 Idempotent consumers
- Before handling, check `processed_events` for `eventId`.
- Insert into `processed_events` in the same transaction as the business change.
- Duplicate events are skipped.

### 10.3 Producer config
- `enable.idempotence=true`, `acks=all`
- JSON serialization for v1, with a stretch goal of Avro plus Schema Registry

### 10.4 Consumer config
- Manual or `RECORD` ack mode, commit after successful processing
- `auto.offset.reset=earliest`
- One consumer group per service: `order-service`, `restaurant-service`, `delivery-service`

### 10.5 Error handling
| Error type | Handling |
|---|---|
| Transient (DB down, timeout) | Retry with backoff via `@RetryableTopic` (e.g. 3 attempts: 1s, 5s, 30s) |
| Invalid state transition | Log and skip, no retry |
| Deserialization / poison message | Send straight to DLT |
| Exhausted retries | DLT, plus an alert log |

---

## 11. REST APIs

### Order Service (`:8081`)
| Method | Path | Description |
|---|---|---|
| POST | `/orders` | Place an order, returns `202 Accepted` with `orderId` |
| GET | `/orders/{id}` | Current status and details |
| GET | `/orders/{id}/history` | Status transition history |
| POST | `/orders/{id}/cancel` | Cancel (409 if too late) |

### Restaurant Service (`:8082`)
| Method | Path | Description |
|---|---|---|
| GET | `/restaurants` | List restaurants |
| PATCH | `/restaurants/{id}` | Toggle open/closed (for testing rejections) |
| POST | `/tickets/{orderId}/ready` | Manually mark ready (or auto after prep time) |

### Delivery Service (`:8083`)
| Method | Path | Description |
|---|---|---|
| GET | `/drivers` | List drivers and status |
| PATCH | `/drivers/{id}` | Set status (set all OFFLINE to test `DeliveryFailed`) |
| POST | `/deliveries/{orderId}/pickup` | Simulate pickup |
| POST | `/deliveries/{orderId}/deliver` | Simulate delivery |

A `simulation.auto-advance=true` flag can make restaurant/delivery steps happen automatically on timers.

---

## 12. Tech stack

- Java 21, Spring Boot 3.x, Spring Kafka
- PostgreSQL (one container, three databases, or three containers)
- Flyway for migrations
- Kafka in KRaft mode via Docker Compose, plus Kafka UI (provectus/kafka-ui)
- Maven multi-module build
- JUnit 5, Testcontainers (Kafka + PostgreSQL), Awaitility
- Observability (stretch): Micrometer + Prometheus + Grafana, consumer lag metrics, OpenTelemetry tracing using `correlationId`

---

## 13. Project structure

```
food-delivery/
├── docker-compose.yml
├── pom.xml                      # parent
├── common-events/               # shared envelope + event DTOs
├── order-service/
├── restaurant-service/
├── delivery-service/
└── docs/
    ├── spec.md
    └── adr/                     # architecture decision records
```

**Open decision:** a shared `common-events` module is convenient but couples services. The alternative is for each service to own its own copy of the DTOs, with schemas as the contract. See Open Questions.

---

## 14. Testing strategy

- **Unit:** state machine transitions, validation logic
- **Integration (per service):** Testcontainers Kafka + Postgres. Publish an input event, assert the output event and DB state.
- **Idempotency test:** publish the same event twice, assert a single side effect.
- **Outbox test:** kill Kafka mid-flow, restart, assert the event is eventually published.
- **End-to-end:** all 3 services plus Kafka in Compose. Run the happy path and each failure saga, asserting the final order status.
- **Ordering test:** publish many events for one `orderId` with 3 consumer instances running, assert the history is in the correct order.

---

## 15. Milestones

| # | Milestone | Kafka concepts | Done when |
|---|---|---|---|
| M1 | Infra: Compose with Kafka (KRaft), Kafka UI, Postgres; multi-module skeleton | Brokers, topics | `docker compose up` works and topics show in Kafka UI |
| M2 | Order Service: POST /orders publishes `OrderPlaced` (direct `KafkaTemplate`) | Producers, keys, partitions | Event visible in Kafka UI with key = orderId |
| M3 | Restaurant Service: consume `OrderPlaced`, publish accept/reject | Consumers, consumer groups | Order status updates to ACCEPTED/REJECTED |
| M4 | Delivery Service + full happy path | Multi-topic choreography | Order reaches DELIVERED |
| M5 | Compensation sagas (no driver, customer cancel) | Saga, compensating events | All failure flows end in the correct state |
| M6 | Idempotency + outbox | At-least-once, dual-write problem | Duplicate and crash tests pass |
| M7 | Retry topics + DLT | Error handling | Poison message lands in DLT, transient errors recover |
| M8 | Scaling: run 3 instances of Restaurant Service | Rebalancing, partition assignment | Load spread across instances, per-order ordering preserved |
| M9 (stretch) | Avro + Schema Registry, event v2 migration | Schema evolution | v1 and v2 consumers coexist |
| M10 (stretch) | Observability: lag dashboards, tracing | Monitoring | Saga visible end-to-end in a trace |

---

## 16. Open questions

1. **Shared event module vs per-service DTOs?** Shared is faster to start; per-service is more realistic.
2. **JSON vs Avro from day one?** JSON is easier to debug; Avro teaches schema evolution.
3. **Should Delivery wait for `OrderReady` before assigning a driver, or assign on `OrderAccepted`?** The spec currently assigns on accept.
4. **Payment step?** Adding a 4th Payment Service would make the saga richer (charge, then refund on failure).
5. **Outbox relay: polling or Debezium?** Polling first, Debezium as an upgrade?
6. **Should Order Service also expose status via SSE/WebSocket** for live tracking?
