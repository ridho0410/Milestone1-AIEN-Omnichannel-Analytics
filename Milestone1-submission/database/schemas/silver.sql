CREATE SCHEMA IF NOT EXISTS silver;
CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id TEXT PRIMARY KEY,
    city_id TEXT,
    customer_segment TEXT,
    created_at_utc TIMESTAMPTZ,
    source_row_id TEXT,
    pipeline_run_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS silver.customer_profiles (
    customer_id TEXT NOT NULL,
    valid_from_utc TIMESTAMPTZ NOT NULL,
    valid_to_utc TIMESTAMPTZ,
    city_id TEXT,
    customer_segment TEXT,
    source_row_id TEXT,
    is_current BOOLEAN NOT NULL DEFAULT FALSE,
    pipeline_run_id TEXT NOT NULL,
    PRIMARY KEY (customer_id, valid_from_utc)
);
CREATE TABLE IF NOT EXISTS silver.customer_addresses (
    address_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    city_id TEXT,
    valid_from_utc TIMESTAMPTZ NOT NULL,
    valid_to_utc TIMESTAMPTZ,
    is_current BOOLEAN NOT NULL DEFAULT FALSE,
    source_row_id TEXT,
    pipeline_run_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS silver.products (
    product_id TEXT PRIMARY KEY,
    product_name TEXT,
    sku TEXT,
    unit_price NUMERIC(14, 2),
    source_row_id TEXT,
    pipeline_run_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS silver.product_categories (
    product_id TEXT NOT NULL,
    valid_from_utc TIMESTAMPTZ NOT NULL,
    valid_to_utc TIMESTAMPTZ,
    category_id TEXT,
    category_name TEXT,
    source_row_id TEXT,
    is_current BOOLEAN NOT NULL DEFAULT FALSE,
    pipeline_run_id TEXT NOT NULL,
    PRIMARY KEY (product_id, valid_from_utc)
);
CREATE TABLE IF NOT EXISTS silver.stores (
    store_id TEXT PRIMARY KEY,
    store_name TEXT,
    city_id TEXT,
    location_type TEXT,
    source_row_id TEXT,
    pipeline_run_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS silver.sales_channels (
    channel_id TEXT PRIMARY KEY,
    channel_name TEXT,
    source_row_id TEXT,
    pipeline_run_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS silver.city_reference (
    city_id TEXT PRIMARY KEY,
    city_name TEXT,
    country TEXT,
    pipeline_run_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS silver.promotions (
    promotion_id TEXT PRIMARY KEY,
    promotion_code TEXT,
    promotion_type TEXT,
    discount_rate NUMERIC(14, 4),
    start_date DATE,
    end_date DATE,
    source_row_id TEXT,
    pipeline_run_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS silver.orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    ordered_at_utc TIMESTAMPTZ NOT NULL,
    order_date DATE NOT NULL,
    sales_channel TEXT NOT NULL,
    store_id TEXT,
    status_raw TEXT,
    order_status TEXT NOT NULL,
    shipping_revenue NUMERIC(14, 2) NOT NULL DEFAULT 0,
    updated_at_utc TIMESTAMPTZ,
    source_row_id TEXT,
    had_duplicate BOOLEAN NOT NULL DEFAULT FALSE,
    pipeline_run_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS silver.order_items (
    order_item_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(14, 2) NOT NULL,
    item_discount_amount NUMERIC(14, 2) NOT NULL DEFAULT 0,
    line_gross NUMERIC(14, 2) NOT NULL,
    line_net NUMERIC(14, 2) NOT NULL,
    source_row_id TEXT,
    pipeline_run_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS silver.order_promotions (
    order_id TEXT NOT NULL,
    promotion_id TEXT NOT NULL,
    promotion_code TEXT,
    applied_discount NUMERIC(14, 2),
    pipeline_run_id TEXT NOT NULL,
    PRIMARY KEY (order_id, promotion_id)
);
CREATE TABLE IF NOT EXISTS silver.payment_events (
    event_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    event_type TEXT,
    payment_status TEXT,
    amount NUMERIC(14, 2),
    occurred_at_utc TIMESTAMPTZ,
    ingested_at_utc TIMESTAMPTZ,
    is_late BOOLEAN NOT NULL DEFAULT FALSE,
    raw_payload JSONB,
    pipeline_run_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS silver.refund_events (
    event_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    event_type TEXT,
    refund_status TEXT,
    amount NUMERIC(14, 2),
    occurred_at_utc TIMESTAMPTZ,
    ingested_at_utc TIMESTAMPTZ,
    is_late BOOLEAN NOT NULL DEFAULT FALSE,
    raw_payload JSONB,
    pipeline_run_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS silver.return_events (
    event_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    return_id TEXT,
    event_type TEXT,
    return_stage TEXT,
    occurred_at_utc TIMESTAMPTZ,
    ingested_at_utc TIMESTAMPTZ,
    is_late BOOLEAN NOT NULL DEFAULT FALSE,
    raw_payload JSONB,
    pipeline_run_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS silver.support_events (
    event_id TEXT PRIMARY KEY,
    ticket_id TEXT,
    customer_id TEXT,
    order_id TEXT,
    reason TEXT,
    event_type TEXT,
    ticket_stage TEXT,
    occurred_at_utc TIMESTAMPTZ,
    ingested_at_utc TIMESTAMPTZ,
    is_late BOOLEAN NOT NULL DEFAULT FALSE,
    raw_payload JSONB,
    pipeline_run_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS silver.web_events (
    event_id TEXT PRIMARY KEY,
    session_id TEXT,
    customer_id TEXT,
    channel TEXT,
    event_type TEXT,
    occurred_at_utc TIMESTAMPTZ,
    ingested_at_utc TIMESTAMPTZ,
    is_late BOOLEAN NOT NULL DEFAULT FALSE,
    raw_payload JSONB,
    pipeline_run_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS silver.inventory_snapshots (
    product_id TEXT NOT NULL,
    location_id TEXT NOT NULL,
    snapshot_date DATE NOT NULL,
    available_quantity INTEGER,
    reserved_quantity INTEGER,
    unit_cost NUMERIC(14, 2),
    pipeline_run_id TEXT NOT NULL,
    PRIMARY KEY (product_id, location_id, snapshot_date)
);
CREATE TABLE IF NOT EXISTS silver.campaign_spend (
    spend_date DATE NOT NULL,
    campaign_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    spend_amount NUMERIC(14, 2) NOT NULL,
    pipeline_run_id TEXT NOT NULL,
    PRIMARY KEY (spend_date, campaign_id, channel)
);
CREATE TABLE IF NOT EXISTS silver.rejected_records (
    rejection_id BIGSERIAL PRIMARY KEY,
    pipeline_run_id TEXT NOT NULL,
    source_file TEXT NOT NULL,
    source_row_number INTEGER,
    source_record_id TEXT,
    reason_code TEXT NOT NULL,
    reason_detail TEXT,
    raw_payload JSONB,
    rejected_at_utc TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_silver_rejected_records_run ON silver.rejected_records (pipeline_run_id, reason_code);