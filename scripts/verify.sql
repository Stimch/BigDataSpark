-- Проверка PostgreSQL (выполнить в DBeaver или psql)

SELECT 'staging.mock_data' AS table_name, COUNT(*) AS row_count FROM staging.mock_data
UNION ALL
SELECT 'dwh.fact_sales', COUNT(*) FROM dwh.fact_sales
UNION ALL
SELECT 'dwh.dim_customer', COUNT(*) FROM dwh.dim_customer
UNION ALL
SELECT 'dwh.dim_product', COUNT(*) FROM dwh.dim_product;

-- Примеры звёздной схемы
SELECT c.customer_country, SUM(f.sale_total_price) AS revenue
FROM dwh.fact_sales f
JOIN dwh.dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.customer_country
ORDER BY revenue DESC
LIMIT 5;
