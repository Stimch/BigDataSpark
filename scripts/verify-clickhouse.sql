-- Проверка витрин в ClickHouse

SELECT 'mart_product_sales' AS mart, section, count() AS rows
FROM lab.mart_product_sales GROUP BY section ORDER BY section;

SELECT 'mart_customer_sales' AS mart, section, count() AS rows
FROM lab.mart_customer_sales GROUP BY section ORDER BY section;

SELECT 'mart_time_sales' AS mart, section, count() AS rows
FROM lab.mart_time_sales GROUP BY section ORDER BY section;

SELECT 'mart_store_sales' AS mart, section, count() AS rows
FROM lab.mart_store_sales GROUP BY section ORDER BY section;

SELECT 'mart_supplier_sales' AS mart, section, count() AS rows
FROM lab.mart_supplier_sales GROUP BY section ORDER BY section;

SELECT 'mart_product_quality' AS mart, section, count() AS rows
FROM lab.mart_product_quality GROUP BY section ORDER BY section;

-- Топ-10 продуктов (витрина 1)
SELECT product_name, total_quantity, total_revenue, rank
FROM lab.mart_product_sales
WHERE section = 'top10_products'
ORDER BY rank;
