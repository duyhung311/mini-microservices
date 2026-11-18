create table orders (
    id               uuid primary key,
    customer_id      uuid          not null,
    restaurant_id    uuid          not null,
    status           varchar(32)   not null,
    total_amount     numeric(12,2) not null,
    delivery_address varchar(500)  not null,
    driver_id        uuid,
    correlation_id   uuid          not null,
    created_at       timestamptz   not null,
    updated_at       timestamptz   not null,
    version          bigint        not null default 0
);

create table order_items (
    id           bigint generated always as identity primary key,
    order_id     uuid          not null references orders (id),
    menu_item_id uuid          not null,
    quantity     int           not null,
    unit_price   numeric(12,2) not null
);
create index idx_order_items_order on order_items (order_id);

create table order_status_history (
    id          bigint generated always as identity primary key,
    order_id    uuid        not null references orders (id),
    from_status varchar(32),
    to_status   varchar(32) not null,
    event_id    uuid,
    changed_at  timestamptz not null
);
create index idx_order_history_order on order_status_history (order_id, id);
