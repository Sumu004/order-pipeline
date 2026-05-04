-- Order Pipeline PostgreSQL Schema
-- Partitioned by created_at month for efficient time-range queries

-- Enable partitioning
ALTER DATABASE order_pipeline SET timezone TO 'UTC';
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;

-- Create customers table
CREATE TABLE customers (
    customer_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create order_items table (non-partitioned, linked to orders)
CREATE TABLE order_items (
    item_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id UUID NOT NULL,
    product_id VARCHAR(100) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create partitioned orders table
-- Primary key must include partition column (created_at)
CREATE TABLE orders (
    order_id UUID NOT NULL,
    customer_id UUID NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    total_amount DECIMAL(12, 2) NOT NULL,
    idempotency_key VARCHAR(255),
    charge_id VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (order_id, created_at)
) PARTITION BY RANGE (created_at);

-- Create default partition
CREATE TABLE orders_default PARTITION OF orders DEFAULT;

-- Seed test customers
INSERT INTO customers (customer_id, email, name) VALUES
    ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'customer1@example.com', 'Alice Johnson'),
    ('b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12', 'customer2@example.com', 'Bob Smith'),
    ('c0eebc99-9c0b-4ef8-bb6d-6bb9bd380a13', 'customer3@example.com', 'Charlie Brown'),
    ('d0eebc99-9c0b-4ef8-bb6d-6bb9bd380a14', 'customer4@example.com', 'Diana Prince'),
    ('e0eebc99-9c0b-4ef8-bb6d-6bb9bd380a15', 'customer5@example.com', 'Eve Williams');

-- Seed 100 test orders
DO $$
DECLARE
    i INTEGER;
    customer_ids UUID[] := ARRAY(
        SELECT customer_id FROM customers LIMIT 5
    );
    statuses TEXT[] := ARRAY['pending', 'processing', 'completed', 'failed'];
BEGIN
    FOR i IN 1..100 LOOP
        INSERT INTO orders (order_id, customer_id, status, total_amount, idempotency_key, created_at)
        VALUES (
            gen_random_uuid(),
            customer_ids[1 + (i % 5)],
            statuses[1 + (i % 4)],
            ROUND((10 + (i * 2.5))::NUMERIC, 2),
            'idem-' || i::TEXT || '-' || LEFT(gen_random_uuid()::TEXT, 8),
            NOW() - INTERVAL '1 day' * (100 - i)
        );
    END LOOP;
END $$;

-- Add order items
DO $$
DECLARE
    order_rec RECORD;
    i INTEGER;
    product_ids TEXT[] := ARRAY['PROD-001', 'PROD-002', 'PROD-003', 'PROD-004', 'PROD-005'];
BEGIN
    FOR order_rec IN SELECT order_id FROM orders LOOP
        FOR i IN 1..(1 + floor(random() * 3)::INTEGER) LOOP
            INSERT INTO order_items (item_id, order_id, product_id, quantity, unit_price)
            VALUES (
                gen_random_uuid(),
                order_rec.order_id,
                product_ids[1 + floor(random() * 5)::INTEGER],
                1 + floor(random() * 3)::INTEGER,
                ROUND((5 + random() * 50)::NUMERIC, 2)
            );
        END LOOP;
    END LOOP;
END $$;

SELECT 'Customers:', COUNT(*) FROM customers
UNION ALL
SELECT 'Orders:', COUNT(*) FROM orders
UNION ALL
SELECT 'Order Items:', COUNT(*) FROM order_items;