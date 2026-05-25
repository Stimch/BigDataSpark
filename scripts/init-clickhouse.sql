-- База и витрины в ClickHouse (заполняются Spark-джобой reports_clickhouse.py)

CREATE DATABASE IF NOT EXISTS lab;

-- 1. Витрина продаж по продуктам
CREATE TABLE IF NOT EXISTS lab.mart_product_sales (
    section String,
    product_name Nullable(String),
    product_category Nullable(String),
    total_quantity Nullable(UInt64),
    total_revenue Nullable(Float64),
    avg_rating Nullable(Float64),
    review_count Nullable(UInt64),
    rank Nullable(UInt32)
) ENGINE = MergeTree ORDER BY (section, ifNull(product_name, ''), ifNull(product_category, ''));

-- 2. Витрина продаж по клиентам
CREATE TABLE IF NOT EXISTS lab.mart_customer_sales (
    section String,
    customer_id Nullable(UInt32),
    customer_name Nullable(String),
    customer_country Nullable(String),
    customers_count Nullable(UInt64),
    total_spent Nullable(Float64),
    avg_check Nullable(Float64),
    rank Nullable(UInt32)
) ENGINE = MergeTree ORDER BY (section, ifNull(customer_id, 0), ifNull(customer_country, ''));

-- 3. Витрина продаж по времени
CREATE TABLE IF NOT EXISTS lab.mart_time_sales (
    section String,
    year Nullable(UInt16),
    month Nullable(UInt8),
    period_label Nullable(String),
    total_revenue Nullable(Float64),
    orders_count Nullable(UInt64),
    avg_order_size Nullable(Float64),
    revenue_prev_period Nullable(Float64),
    revenue_change_pct Nullable(Float64)
) ENGINE = MergeTree ORDER BY (section, ifNull(year, 0), ifNull(month, 0));

-- 4. Витрина продаж по магазинам
CREATE TABLE IF NOT EXISTS lab.mart_store_sales (
    section String,
    store_name Nullable(String),
    store_city Nullable(String),
    store_country Nullable(String),
    total_revenue Nullable(Float64),
    orders_count Nullable(UInt64),
    avg_check Nullable(Float64),
    rank Nullable(UInt32)
) ENGINE = MergeTree ORDER BY (section, ifNull(store_name, ''), ifNull(store_city, ''));

-- 5. Витрина продаж по поставщикам
CREATE TABLE IF NOT EXISTS lab.mart_supplier_sales (
    section String,
    supplier_name Nullable(String),
    supplier_country Nullable(String),
    total_revenue Nullable(Float64),
    avg_product_price Nullable(Float64),
    products_sold Nullable(UInt64),
    rank Nullable(UInt32)
) ENGINE = MergeTree ORDER BY (section, ifNull(supplier_name, ''), ifNull(supplier_country, ''));

-- 6. Витрина качества продукции
CREATE TABLE IF NOT EXISTS lab.mart_product_quality (
    section String,
    product_name Nullable(String),
    product_rating Nullable(Float64),
    total_quantity_sold Nullable(UInt64),
    review_count Nullable(UInt64),
    rating_sales_correlation Nullable(Float64),
    rank Nullable(UInt32)
) ENGINE = MergeTree ORDER BY (section, ifNull(product_name, ''), ifNull(product_rating, 0));
