CREATE DATABASE airflow;

\connect Ecommerce;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE TABLE IF NOT EXISTS bronze.events_raw (
    event_id UUID NOT NULL,
    topic_name TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    ingestion_date DATE NOT NULL,
    payload JSONB NOT NULL,
    schema_version TEXT NOT NULL DEFAULT '1.0',
    source_service TEXT NOT NULL,
    ingestion_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, ingestion_date)
) PARTITION BY RANGE (ingestion_date);

CREATE TABLE IF NOT EXISTS bronze.events_raw_default
PARTITION OF bronze.events_raw DEFAULT;

CREATE INDEX IF NOT EXISTS idx_events_raw_topic_ingestion ON bronze.events_raw (topic_name, ingestion_date);
CREATE INDEX IF NOT EXISTS idx_events_raw_event_ts ON bronze.events_raw (event_timestamp);
CREATE INDEX IF NOT EXISTS idx_events_raw_payload_gin ON bronze.events_raw USING GIN (payload);

CREATE TABLE IF NOT EXISTS bronze.fraud_alerts (
    alert_id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    order_id TEXT,
    user_id TEXT,
    amount NUMERIC(12,2) NOT NULL,
    risk_score NUMERIC(5,2) NOT NULL,
    reason TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fraud_alerts_event_ts ON bronze.fraud_alerts (event_timestamp);

CREATE TABLE IF NOT EXISTS bronze.inventory_alerts (
    alert_id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    product_id TEXT NOT NULL,
    sku TEXT NOT NULL,
    stock_level INTEGER NOT NULL,
    threshold INTEGER NOT NULL,
    warehouse_id TEXT,
    alert_message TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_inventory_alerts_product ON bronze.inventory_alerts (product_id, event_timestamp);

CREATE TABLE IF NOT EXISTS silver.users_clean (
    user_id TEXT PRIMARY KEY,
    email TEXT NOT NULL,
    full_name TEXT NOT NULL,
    city TEXT,
    country TEXT,
    signup_ts TIMESTAMPTZ,
    is_active BOOLEAN NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver.products_clean (
    product_id TEXT PRIMARY KEY,
    sku TEXT NOT NULL UNIQUE,
    product_name TEXT NOT NULL,
    category TEXT,
    unit_price NUMERIC(12,2) NOT NULL,
    is_active BOOLEAN NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver.orders_clean (
    order_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(12,2) NOT NULL,
    total_amount NUMERIC(12,2) NOT NULL,
    currency TEXT NOT NULL,
    order_status TEXT NOT NULL,
    order_timestamp TIMESTAMPTZ NOT NULL,
    source_event_id UUID NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver.payments_clean (
    payment_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    currency TEXT NOT NULL,
    payment_method TEXT NOT NULL,
    payment_status TEXT NOT NULL,
    payment_timestamp TIMESTAMPTZ NOT NULL,
    source_event_id UUID NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver.shipping_clean (
    shipment_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    carrier TEXT NOT NULL,
    shipping_status TEXT NOT NULL,
    estimated_delivery_date DATE,
    shipped_timestamp TIMESTAMPTZ,
    delivered_timestamp TIMESTAMPTZ,
    source_event_id UUID NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_clean_user_ts ON silver.orders_clean (user_id, order_timestamp);
CREATE INDEX IF NOT EXISTS idx_payments_clean_order_ts ON silver.payments_clean (order_id, payment_timestamp);
CREATE INDEX IF NOT EXISTS idx_shipping_clean_order_ts ON silver.shipping_clean (order_id, shipped_timestamp);

CREATE TABLE IF NOT EXISTS gold.dim_users (
    user_sk BIGSERIAL PRIMARY KEY,
    user_id TEXT NOT NULL UNIQUE,
    email TEXT NOT NULL,
    full_name TEXT NOT NULL,
    city TEXT,
    country TEXT,
    signup_ts TIMESTAMPTZ,
    is_active BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.dim_products (
    product_sk BIGSERIAL PRIMARY KEY,
    product_id TEXT NOT NULL UNIQUE,
    sku TEXT NOT NULL,
    product_name TEXT NOT NULL,
    category TEXT,
    unit_price NUMERIC(12,2) NOT NULL,
    is_active BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_sk INTEGER PRIMARY KEY,
    date_day DATE NOT NULL UNIQUE,
    day_of_week INTEGER NOT NULL,
    month INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    year INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.fact_orders (
    order_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    order_date DATE NOT NULL,
    quantity INTEGER NOT NULL,
    total_amount NUMERIC(12,2) NOT NULL,
    order_status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_fact_orders_user FOREIGN KEY (user_id) REFERENCES gold.dim_users(user_id),
    CONSTRAINT fk_fact_orders_product FOREIGN KEY (product_id) REFERENCES gold.dim_products(product_id)
);

CREATE TABLE IF NOT EXISTS gold.fact_payments (
    payment_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    payment_date DATE NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    payment_method TEXT NOT NULL,
    payment_status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_fact_payments_order FOREIGN KEY (order_id) REFERENCES gold.fact_orders(order_id)
);

CREATE TABLE IF NOT EXISTS gold.fact_shipping (
    shipment_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    shipping_status TEXT NOT NULL,
    carrier TEXT NOT NULL,
    shipped_date DATE,
    delivered_date DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_fact_shipping_order FOREIGN KEY (order_id) REFERENCES gold.fact_orders(order_id)
);

CREATE TABLE IF NOT EXISTS monitoring.pipeline_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    metric_name TEXT NOT NULL,
    metric_value NUMERIC(18,2) NOT NULL,
    metric_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_name_ts ON monitoring.pipeline_metrics (metric_name, metric_timestamp);
