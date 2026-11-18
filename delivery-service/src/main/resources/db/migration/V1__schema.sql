CREATE TABLE drivers (
    id     uuid         PRIMARY KEY,
    name   varchar(100) NOT NULL,
    status varchar(20)  NOT NULL
);

CREATE TABLE deliveries (
    id              uuid         PRIMARY KEY,
    order_id        uuid         NOT NULL UNIQUE,
    driver_id       uuid         NULL REFERENCES drivers (id),
    status          varchar(30)  NOT NULL,
    pickup_address  varchar(500) NULL,
    dropoff_address varchar(500) NULL,
    created_at      timestamptz  NOT NULL,
    updated_at      timestamptz  NOT NULL,
    correlation_id  uuid         NOT NULL,
    last_event_id   uuid         NOT NULL
);

CREATE INDEX idx_deliveries_status_updated ON deliveries (status, updated_at);
CREATE INDEX idx_deliveries_driver ON deliveries (driver_id);
