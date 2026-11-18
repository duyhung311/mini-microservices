CREATE TABLE restaurants (
    id                    uuid PRIMARY KEY,
    name                  varchar(200) NOT NULL,
    address               varchar(500) NOT NULL,
    is_open               boolean      NOT NULL,
    max_concurrent_orders integer      NOT NULL CHECK (max_concurrent_orders >= 0)
);

CREATE TABLE menu_items (
    id            uuid PRIMARY KEY,
    restaurant_id uuid           NOT NULL REFERENCES restaurants (id),
    name          varchar(200)   NOT NULL,
    price         numeric(12, 2) NOT NULL,
    available     boolean        NOT NULL
);
CREATE INDEX idx_menu_items_restaurant ON menu_items (restaurant_id);

-- correlation_id and cause_event_id are additive columns: they let OrderReady be built with
-- Envelopes.next from the original OrderPlaced when REST or the scheduler triggers it.
CREATE TABLE kitchen_tickets (
    id             uuid PRIMARY KEY,
    order_id       uuid         NOT NULL UNIQUE,
    restaurant_id  uuid         NOT NULL REFERENCES restaurants (id),
    status         varchar(20)  NOT NULL,
    created_at     timestamptz  NOT NULL,
    correlation_id uuid         NOT NULL,
    cause_event_id uuid         NOT NULL
);
CREATE INDEX idx_tickets_status_created ON kitchen_tickets (status, created_at);
CREATE INDEX idx_tickets_restaurant_status ON kitchen_tickets (restaurant_id, status);
