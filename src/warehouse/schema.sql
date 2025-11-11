-- Data Warehouse Schema
-- Optimized for analytics queries

CREATE DATABASE warehouse;
\c warehouse;

-- Fact table for order events (optimized for analytics)
CREATE TABLE IF NOT EXISTS order_events (
    event_id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    total_amount DECIMAL(12, 2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    order_date TIMESTAMP NOT NULL,
    status VARCHAR(50) NOT NULL,
    event_type VARCHAR(20) NOT NULL, -- INSERT, UPDATE, DELETE
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_timestamp TIMESTAMP NOT NULL,
    
    -- Indexing for analytics
    INDEX idx_customer_id (customer_id),
    INDEX idx_product_name (product_name),
    INDEX idx_order_date (order_date),
    INDEX idx_status (status),
    INDEX idx_processed_at (processed_at)
);

-- Dimension table for customers (for future expansion)
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id INTEGER PRIMARY KEY,
    customer_name VARCHAR(255),
    customer_email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Summary table for daily order metrics
CREATE TABLE IF NOT EXISTS daily_order_summary (
    summary_date DATE PRIMARY KEY,
    total_orders INTEGER NOT NULL DEFAULT 0,
    total_revenue DECIMAL(15, 2) NOT NULL DEFAULT 0,
    avg_order_value DECIMAL(10, 2) NOT NULL DEFAULT 0,
    unique_customers INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);