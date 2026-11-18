INSERT INTO restaurants (id, name, address, is_open, max_concurrent_orders) VALUES
    ('11111111-1111-1111-1111-111111111111', 'Pasta Place', '1 Kitchen St', true, 5),
    ('11111111-1111-1111-1111-111111111112', 'Night Grill', '2 Midnight Ave', false, 5);

INSERT INTO menu_items (id, restaurant_id, name, price, available) VALUES
    ('22222222-2222-2222-2222-000000000001', '11111111-1111-1111-1111-111111111111', 'Margherita', 12.50, true),
    ('22222222-2222-2222-2222-000000000002', '11111111-1111-1111-1111-111111111111', 'Lasagne', 14.00, true),
    ('22222222-2222-2222-2222-000000000003', '11111111-1111-1111-1111-111111111111', 'Truffle Special', 30.00, false);
