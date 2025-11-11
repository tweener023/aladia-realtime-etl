-- Source Database Schema
-- PostgreSQL setup for CDC-enabled orders table

-- Enable logical replication
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_replication_slots = 4;
ALTER SYSTEM SET max_wal_senders = 4;

-- Create database if not exists
CREATE DATABASE sourcedb;

-- Connect to sourcedb
\c sourcedb;

-- Create orders table with CDC support
CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update updated_at
CREATE TRIGGER update_orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Enable replica identity for full CDC support
ALTER TABLE orders REPLICA IDENTITY FULL;

-- Insert sample data
INSERT INTO orders (customer_id, product_name, quantity, unit_price, status) VALUES
(1, 'MacBook Pro', 1, 1999.99, 'pending'),
(2, 'iPhone 14', 2, 999.99, 'confirmed'),
(3, 'AirPods Pro', 1, 249.99, 'shipped'),
(4, 'iPad Air', 1, 599.99, 'delivered'),
(5, 'Samsung Galaxy S23', 1, 899.99, 'pending'),
(6, 'Dell XPS 13', 1, 1299.99, 'confirmed'),
(7, 'Sony WH-1000XM4', 1, 349.99, 'pending'),
(8, 'Nintendo Switch', 2, 299.99, 'confirmed'),
(9, 'Tesla Model 3', 1, 45000.00, 'pending'),
(10, 'Apple Watch', 1, 399.99, 'confirmed');