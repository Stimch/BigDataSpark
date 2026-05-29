"""
Построение 6 аналитических витрин из модели «звезда» (PostgreSQL) в ClickHouse.
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import urllib.parse
import urllib.request

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from config import (
    CLICKHOUSE_DB,
    CLICKHOUSE_HOST,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_PROPERTIES,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
    JDBC_CLICKHOUSE_URL,
    JDBC_POSTGRES_URL,
    POSTGRES_PROPERTIES,
    SPARK_JARS,
)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("lab2-reports-clickhouse")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.jars", SPARK_JARS)
        .getOrCreate()
    )


def read_star_schema(spark: SparkSession):
    query = """
        SELECT
            f.sale_id,
            f.sale_quantity,
            f.sale_total_price,
            f.product_price,
            c.customer_id,
            c.customer_first_name,
            c.customer_last_name,
            c.customer_country,
            p.product_id,
            p.product_name,
            p.product_category,
            p.product_rating,
            p.product_reviews,
            s.store_name,
            s.store_city,
            s.store_country,
            su.supplier_name,
            su.supplier_country,
            d.sale_date,
            d.year,
            d.month,
            d.quarter
        FROM dwh.fact_sales f
        JOIN dwh.dim_customer c ON f.customer_key = c.customer_key
        JOIN dwh.dim_product p ON f.product_key = p.product_key
        JOIN dwh.dim_store s ON f.store_key = s.store_key
        JOIN dwh.dim_supplier su ON f.supplier_key = su.supplier_key
        JOIN dwh.dim_date d ON f.date_key = d.date_key
    """
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_POSTGRES_URL)
        .option("query", query)
        .options(**POSTGRES_PROPERTIES)
        .load()
    )


def _table_name(table: str) -> str:
    """Имя таблицы без префикса БД (db задаётся в URL и параметре database)."""
    return table.rsplit(".", 1)[-1]


def write_clickhouse(df, table: str) -> None:
    (
        df.write.format("jdbc")
        .option("url", JDBC_CLICKHOUSE_URL)
        .option("dbtable", _table_name(table))
        .mode("append")
        .options(**CLICKHOUSE_PROPERTIES)
        .save()
    )


def truncate_clickhouse_table(table: str) -> None:
    # Не использовать lab.table при database=lab в URL — иначе HTTP 500.
    sql = f"TRUNCATE TABLE IF EXISTS {_table_name(table)}"
    params = urllib.parse.urlencode(
        {
            "database": CLICKHOUSE_DB,
            "query": sql,
            "user": CLICKHOUSE_USER,
            "password": CLICKHOUSE_PASSWORD,
        }
    )
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/?{params}"
    with urllib.request.urlopen(url, timeout=60) as response:
        response.read()


def build_product_mart(sales):
    top10 = (
        sales.groupBy("product_name", "product_category")
        .agg(
            F.sum("sale_quantity").alias("total_quantity"),
            F.sum("sale_total_price").alias("total_revenue"),
        )
        .withColumn(
            "rank",
            F.row_number().over(Window.orderBy(F.desc("total_quantity"))),
        )
        .where(F.col("rank") <= 10)
        .select(
            F.lit("top10_products").alias("section"),
            "product_name",
            "product_category",
            "total_quantity",
            "total_revenue",
            F.lit(None).cast("double").alias("avg_rating"),
            F.lit(None).cast("long").alias("review_count"),
            "rank",
        )
    )

    by_category = (
        sales.groupBy("product_category")
        .agg(F.sum("sale_total_price").alias("total_revenue"))
        .select(
            F.lit("category_revenue").alias("section"),
            F.lit(None).cast("string").alias("product_name"),
            F.col("product_category"),
            F.lit(None).cast("long").alias("total_quantity"),
            "total_revenue",
            F.lit(None).cast("double").alias("avg_rating"),
            F.lit(None).cast("long").alias("review_count"),
            F.lit(None).cast("int").alias("rank"),
        )
    )

    ratings = (
        sales.groupBy("product_name", "product_category")
        .agg(
            F.avg("product_rating").alias("avg_rating"),
            F.max("product_reviews").alias("review_count"),
        )
        .select(
            F.lit("product_ratings").alias("section"),
            "product_name",
            "product_category",
            F.lit(None).cast("long").alias("total_quantity"),
            F.lit(None).cast("double").alias("total_revenue"),
            "avg_rating",
            "review_count",
            F.lit(None).cast("int").alias("rank"),
        )
    )

    return top10.unionByName(by_category).unionByName(ratings)


def build_customer_mart(sales):
    customer_name = F.concat_ws(
        " ", F.col("customer_first_name"), F.col("customer_last_name")
    )

    per_customer = sales.groupBy(
        "customer_id", "customer_first_name", "customer_last_name", "customer_country"
    ).agg(F.sum("sale_total_price").alias("total_spent"))

    top10 = (
        per_customer.withColumn("customer_name", customer_name)
        .withColumn("rank", F.row_number().over(Window.orderBy(F.desc("total_spent"))))
        .where(F.col("rank") <= 10)
        .select(
            F.lit("top10_customers").alias("section"),
            "customer_id",
            "customer_name",
            "customer_country",
            F.lit(None).cast("long").alias("customers_count"),
            "total_spent",
            F.lit(None).cast("double").alias("avg_check"),
            "rank",
        )
    )

    by_country = (
        sales.groupBy("customer_country")
        .agg(F.countDistinct("customer_id").alias("customers_count"))
        .select(
            F.lit("customers_by_country").alias("section"),
            F.lit(None).cast("int").alias("customer_id"),
            F.lit(None).cast("string").alias("customer_name"),
            F.col("customer_country"),
            "customers_count",
            F.lit(None).cast("double").alias("total_spent"),
            F.lit(None).cast("double").alias("avg_check"),
            F.lit(None).cast("int").alias("rank"),
        )
    )

    orders_per_customer = sales.groupBy(
        "customer_id", "customer_first_name", "customer_last_name", "customer_country"
    ).agg(
        F.sum("sale_total_price").alias("total_spent"),
        F.count("sale_id").alias("orders_count"),
    )
    avg_check_fixed = (
        orders_per_customer.withColumn(
            "customer_name",
            F.concat_ws(" ", "customer_first_name", "customer_last_name"),
        )
        .withColumn("avg_check", F.col("total_spent") / F.col("orders_count"))
        .select(
            F.lit("avg_check_per_customer").alias("section"),
            "customer_id",
            "customer_name",
            "customer_country",
            F.lit(None).cast("long").alias("customers_count"),
            "total_spent",
            "avg_check",
            F.lit(None).cast("int").alias("rank"),
        )
    )

    return top10.unionByName(by_country).unionByName(avg_check_fixed)


def build_time_mart(sales):
    monthly = (
        sales.groupBy("year", "month")
        .agg(
            F.sum("sale_total_price").alias("total_revenue"),
            F.count("sale_id").alias("orders_count"),
            F.avg("sale_total_price").alias("avg_order_size"),
        )
        .withColumn(
            "period_label",
            F.concat_ws("-", F.col("year").cast("string"), F.lpad(F.col("month").cast("string"), 2, "0")),
        )
        .orderBy("year", "month")
    )

    w = Window.orderBy("year", "month")
    monthly_trends = (
        monthly.withColumn("revenue_prev_period", F.lag("total_revenue").over(w))
        .withColumn(
            "revenue_change_pct",
            F.when(
                F.col("revenue_prev_period").isNull() | (F.col("revenue_prev_period") == 0),
                F.lit(None),
            ).otherwise(
                (F.col("total_revenue") - F.col("revenue_prev_period"))
                / F.col("revenue_prev_period")
                * 100
            ),
        )
        .select(
            F.lit("monthly_trends").alias("section"),
            "year",
            "month",
            "period_label",
            "total_revenue",
            "orders_count",
            "avg_order_size",
            "revenue_prev_period",
            "revenue_change_pct",
        )
    )

    yearly = (
        sales.groupBy("year")
        .agg(
            F.sum("sale_total_price").alias("total_revenue"),
            F.count("sale_id").alias("orders_count"),
            F.avg("sale_total_price").alias("avg_order_size"),
        )
        .withColumn("month", F.lit(None).cast("int"))
        .withColumn("period_label", F.col("year").cast("string"))
        .withColumn("revenue_prev_period", F.lag("total_revenue").over(Window.orderBy("year")))
        .withColumn(
            "revenue_change_pct",
            F.when(
                F.col("revenue_prev_period").isNull() | (F.col("revenue_prev_period") == 0),
                F.lit(None),
            ).otherwise(
                (F.col("total_revenue") - F.col("revenue_prev_period"))
                / F.col("revenue_prev_period")
                * 100
            ),
        )
        .select(
            F.lit("yearly_trends").alias("section"),
            "year",
            "month",
            "period_label",
            "total_revenue",
            "orders_count",
            "avg_order_size",
            "revenue_prev_period",
            "revenue_change_pct",
        )
    )

    period_compare = monthly_trends.where(F.col("revenue_prev_period").isNotNull()).select(
        F.lit("period_comparison").alias("section"),
        "year",
        "month",
        "period_label",
        "total_revenue",
        "orders_count",
        F.lit(None).cast("double").alias("avg_order_size"),
        "revenue_prev_period",
        "revenue_change_pct",
    )

    avg_order_by_month = monthly.select(
        F.lit("avg_order_by_month").alias("section"),
        "year",
        "month",
        "period_label",
        F.lit(None).cast("double").alias("total_revenue"),
        "orders_count",
        "avg_order_size",
        F.lit(None).cast("double").alias("revenue_prev_period"),
        F.lit(None).cast("double").alias("revenue_change_pct"),
    )

    return monthly_trends.unionByName(yearly).unionByName(period_compare).unionByName(
        avg_order_by_month
    )


def build_store_mart(sales):
    per_store = sales.groupBy("store_name", "store_city", "store_country").agg(
        F.sum("sale_total_price").alias("total_revenue"),
        F.count("sale_id").alias("orders_count"),
    )

    top5 = (
        per_store.withColumn("rank", F.row_number().over(Window.orderBy(F.desc("total_revenue"))))
        .where(F.col("rank") <= 5)
        .withColumn("avg_check", F.col("total_revenue") / F.col("orders_count"))
        .select(
            F.lit("top5_stores").alias("section"),
            "store_name",
            "store_city",
            "store_country",
            "total_revenue",
            "orders_count",
            "avg_check",
            "rank",
        )
    )

    by_location = (
        sales.groupBy("store_city", "store_country")
        .agg(
            F.sum("sale_total_price").alias("total_revenue"),
            F.count("sale_id").alias("orders_count"),
        )
        .select(
            F.lit("sales_by_location").alias("section"),
            F.lit(None).cast("string").alias("store_name"),
            "store_city",
            "store_country",
            "total_revenue",
            "orders_count",
            F.lit(None).cast("double").alias("avg_check"),
            F.lit(None).cast("int").alias("rank"),
        )
    )

    avg_check = (
        per_store.withColumn("avg_check", F.col("total_revenue") / F.col("orders_count"))
        .select(
            F.lit("avg_check_per_store").alias("section"),
            "store_name",
            "store_city",
            "store_country",
            "total_revenue",
            "orders_count",
            "avg_check",
            F.lit(None).cast("int").alias("rank"),
        )
    )

    return top5.unionByName(by_location).unionByName(avg_check)


def build_supplier_mart(sales):
    per_supplier = sales.groupBy("supplier_name", "supplier_country").agg(
        F.sum("sale_total_price").alias("total_revenue"),
        F.avg("product_price").alias("avg_product_price"),
        F.sum("sale_quantity").alias("products_sold"),
    )

    top5 = (
        per_supplier.withColumn("rank", F.row_number().over(Window.orderBy(F.desc("total_revenue"))))
        .where(F.col("rank") <= 5)
        .select(
            F.lit("top5_suppliers").alias("section"),
            "supplier_name",
            "supplier_country",
            "total_revenue",
            "avg_product_price",
            "products_sold",
            "rank",
        )
    )

    avg_price = per_supplier.select(
        F.lit("avg_price_per_supplier").alias("section"),
        "supplier_name",
        "supplier_country",
        F.lit(None).cast("double").alias("total_revenue"),
        "avg_product_price",
        F.lit(None).cast("long").alias("products_sold"),
        F.lit(None).cast("int").alias("rank"),
    )

    by_country = (
        sales.groupBy("supplier_country")
        .agg(
            F.sum("sale_total_price").alias("total_revenue"),
            F.sum("sale_quantity").alias("products_sold"),
        )
        .select(
            F.lit("sales_by_supplier_country").alias("section"),
            F.lit(None).cast("string").alias("supplier_name"),
            "supplier_country",
            "total_revenue",
            F.lit(None).cast("double").alias("avg_product_price"),
            "products_sold",
            F.lit(None).cast("int").alias("rank"),
        )
    )

    return top5.unionByName(avg_price).unionByName(by_country)


def build_quality_mart(sales):
    product_stats = sales.groupBy("product_name").agg(
        F.max("product_rating").alias("product_rating"),
        F.sum("sale_quantity").alias("total_quantity_sold"),
        F.max("product_reviews").alias("review_count"),
    )

    w_rating = Window.orderBy(F.desc("product_rating"))
    highest = (
        product_stats.withColumn("rank", F.row_number().over(w_rating))
        .where(F.col("rank") <= 10)
        .select(
            F.lit("highest_rating").alias("section"),
            "product_name",
            "product_rating",
            "total_quantity_sold",
            "review_count",
            F.lit(None).cast("double").alias("rating_sales_correlation"),
            "rank",
        )
    )

    lowest = (
        product_stats.withColumn("rank", F.row_number().over(Window.orderBy("product_rating")))
        .where(F.col("rank") <= 10)
        .select(
            F.lit("lowest_rating").alias("section"),
            "product_name",
            "product_rating",
            "total_quantity_sold",
            "review_count",
            F.lit(None).cast("double").alias("rating_sales_correlation"),
            "rank",
        )
    )

    corr_row = (
        sales.select(
            F.corr("product_rating", "sale_quantity").alias("rating_sales_correlation")
        )
        .select(
            F.lit("rating_sales_correlation").alias("section"),
            F.lit(None).cast("string").alias("product_name"),
            F.lit(None).cast("double").alias("product_rating"),
            F.lit(None).cast("long").alias("total_quantity_sold"),
            F.lit(None).cast("long").alias("review_count"),
            "rating_sales_correlation",
            F.lit(None).cast("int").alias("rank"),
        )
    )

    most_reviews = (
        product_stats.withColumn(
            "rank", F.row_number().over(Window.orderBy(F.desc("review_count")))
        )
        .where(F.col("rank") <= 10)
        .select(
            F.lit("most_reviews").alias("section"),
            "product_name",
            "product_rating",
            "total_quantity_sold",
            "review_count",
            F.lit(None).cast("double").alias("rating_sales_correlation"),
            "rank",
        )
    )

    return highest.unionByName(lowest).unionByName(corr_row).unionByName(most_reviews)


MART_BUILDERS = [
    ("mart_product_sales", build_product_mart),
    ("mart_customer_sales", build_customer_mart),
    ("mart_time_sales", build_time_mart),
    ("mart_store_sales", build_store_mart),
    ("mart_supplier_sales", build_supplier_mart),
    ("mart_product_quality", build_quality_mart),
]


def main() -> None:
    spark = build_spark()
    try:
        sales = read_star_schema(spark)
        if sales.count() == 0:
            raise RuntimeError("Star schema is empty. Run etl_star_schema.py first.")

        for table, builder in MART_BUILDERS:
            truncate_clickhouse_table(table)
            mart_df = builder(sales)
            write_clickhouse(mart_df, table)
            print(f"Loaded {mart_df.count()} rows into {CLICKHOUSE_DB}.{table}")

        print("All ClickHouse marts built successfully")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
