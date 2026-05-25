$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

Write-Host "==> Starting infrastructure..."
docker compose up -d postgres clickhouse
docker compose up clickhouse-init data-loader

Write-Host "==> Building Spark image (if needed)..."
docker compose build spark
docker compose up -d spark

$jars = "/opt/spark/jars-extra/postgresql-42.7.3.jar,/opt/spark/jars-extra/clickhouse-jdbc-0.6.5-all.jar"

Write-Host "==> Running ETL: star schema in PostgreSQL..."
docker compose exec spark /opt/spark/bin/spark-submit --jars $jars /opt/spark/work-dir/jobs/etl_star_schema.py

Write-Host "==> Building ClickHouse marts..."
docker compose exec spark /opt/spark/bin/spark-submit --jars $jars /opt/spark/work-dir/jobs/reports_clickhouse.py

Write-Host "==> Pipeline finished."
