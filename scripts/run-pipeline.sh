#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "==> Starting infrastructure..."
docker compose up -d postgres clickhouse
docker compose up clickhouse-init data-loader

echo "==> Building Spark image (if needed)..."
docker compose build spark
docker compose up -d spark

echo "==> Running ETL: star schema in PostgreSQL..."
docker compose exec spark /opt/spark/bin/spark-submit \
  --jars "${SPARK_JARS:-/opt/spark/jars-extra/postgresql-42.7.3.jar,/opt/spark/jars-extra/clickhouse-jdbc-0.6.5-all.jar}" \
  /opt/spark/work-dir/jobs/etl_star_schema.py

echo "==> Building ClickHouse marts..."
docker compose exec spark /opt/spark/bin/spark-submit \
  --jars "${SPARK_JARS:-/opt/spark/jars-extra/postgresql-42.7.3.jar,/opt/spark/jars-extra/clickhouse-jdbc-0.6.5-all.jar}" \
  /opt/spark/work-dir/jobs/reports_clickhouse.py

echo "==> Pipeline finished. Sample checks:"
echo "PostgreSQL: SELECT COUNT(*) FROM dwh.fact_sales;"
echo "ClickHouse: SELECT section, count() FROM lab.mart_product_sales GROUP BY section;"
