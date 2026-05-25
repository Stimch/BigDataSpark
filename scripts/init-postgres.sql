-- Схема для лабораторной работы №2 (Big Data / Spark ETL)

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;

-- Исходная таблица (заполняется скриптом load_mock_data.py)
CREATE TABLE IF NOT EXISTS staging.mock_data (
    id INTEGER PRIMARY KEY,
    customer_first_name TEXT,
    customer_last_name TEXT,
    customer_age INTEGER,
    customer_email TEXT,
    customer_country TEXT,
    customer_postal_code TEXT,
    customer_pet_type TEXT,
    customer_pet_name TEXT,
    customer_pet_breed TEXT,
    seller_first_name TEXT,
    seller_last_name TEXT,
    seller_email TEXT,
    seller_country TEXT,
    seller_postal_code TEXT,
    product_name TEXT,
    product_category TEXT,
    product_price NUMERIC(12, 2),
    product_quantity INTEGER,
    sale_date TEXT,
    sale_customer_id INTEGER,
    sale_seller_id INTEGER,
    sale_product_id INTEGER,
    sale_quantity INTEGER,
    sale_total_price NUMERIC(12, 2),
    store_name TEXT,
    store_location TEXT,
    store_city TEXT,
    store_state TEXT,
    store_country TEXT,
    store_phone TEXT,
    store_email TEXT,
    pet_category TEXT,
    product_weight NUMERIC(12, 2),
    product_color TEXT,
    product_size TEXT,
    product_brand TEXT,
    product_material TEXT,
    product_description TEXT,
    product_rating NUMERIC(4, 2),
    product_reviews INTEGER,
    product_release_date TEXT,
    product_expiry_date TEXT,
    supplier_name TEXT,
    supplier_contact TEXT,
    supplier_email TEXT,
    supplier_phone TEXT,
    supplier_address TEXT,
    supplier_city TEXT,
    supplier_country TEXT
);

-- Модель «звезда» в PostgreSQL
CREATE TABLE IF NOT EXISTS dwh.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL UNIQUE,
    customer_first_name TEXT,
    customer_last_name TEXT,
    customer_age INTEGER,
    customer_email TEXT,
    customer_country TEXT,
    customer_postal_code TEXT,
    customer_pet_type TEXT,
    customer_pet_name TEXT,
    customer_pet_breed TEXT
);

CREATE TABLE IF NOT EXISTS dwh.dim_seller (
    seller_key SERIAL PRIMARY KEY,
    seller_id INTEGER NOT NULL UNIQUE,
    seller_first_name TEXT,
    seller_last_name TEXT,
    seller_email TEXT,
    seller_country TEXT,
    seller_postal_code TEXT
);

CREATE TABLE IF NOT EXISTS dwh.dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL UNIQUE,
    product_name TEXT,
    product_category TEXT,
    product_price NUMERIC(12, 2),
    product_brand TEXT,
    product_material TEXT,
    product_color TEXT,
    product_size TEXT,
    product_weight NUMERIC(12, 2),
    product_rating NUMERIC(4, 2),
    product_reviews INTEGER,
    pet_category TEXT
);

CREATE TABLE IF NOT EXISTS dwh.dim_store (
    store_key SERIAL PRIMARY KEY,
    store_name TEXT NOT NULL,
    store_location TEXT,
    store_city TEXT,
    store_state TEXT,
    store_country TEXT,
    store_phone TEXT,
    store_email TEXT,
    UNIQUE (store_name, store_city, store_country)
);

CREATE TABLE IF NOT EXISTS dwh.dim_supplier (
    supplier_key SERIAL PRIMARY KEY,
    supplier_name TEXT NOT NULL UNIQUE,
    supplier_contact TEXT,
    supplier_email TEXT,
    supplier_phone TEXT,
    supplier_address TEXT,
    supplier_city TEXT,
    supplier_country TEXT
);

CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_key INTEGER PRIMARY KEY,
    sale_date DATE NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.fact_sales (
    sale_id INTEGER PRIMARY KEY,
    customer_key INTEGER NOT NULL REFERENCES dwh.dim_customer(customer_key),
    seller_key INTEGER NOT NULL REFERENCES dwh.dim_seller(seller_key),
    product_key INTEGER NOT NULL REFERENCES dwh.dim_product(product_key),
    store_key INTEGER NOT NULL REFERENCES dwh.dim_store(store_key),
    supplier_key INTEGER NOT NULL REFERENCES dwh.dim_supplier(supplier_key),
    date_key INTEGER NOT NULL REFERENCES dwh.dim_date(date_key),
    sale_quantity INTEGER NOT NULL,
    sale_total_price NUMERIC(12, 2) NOT NULL,
    product_price NUMERIC(12, 2) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON dwh.fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON dwh.fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON dwh.fact_sales(customer_key);
