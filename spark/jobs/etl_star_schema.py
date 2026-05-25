"""
ETL: staging.mock_data (PostgreSQL) -> модель «звезда» в схеме dwh (PostgreSQL).
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import psycopg2
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from config import (
    JDBC_POSTGRES_URL,
    POSTGRES_PROPERTIES,
    SPARK_JARS,
)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("lab2-etl-star-schema")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.jars", SPARK_JARS)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )


def read_mock_data(spark: SparkSession):
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_POSTGRES_URL)
        .option("dbtable", "staging.mock_data")
        .options(**POSTGRES_PROPERTIES)
        .load()
    )


def truncate_dwh_tables() -> None:
    from config import POSTGRES_DB, POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_USER

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                TRUNCATE TABLE
                    dwh.fact_sales,
                    dwh.dim_customer,
                    dwh.dim_seller,
                    dwh.dim_product,
                    dwh.dim_store,
                    dwh.dim_supplier,
                    dwh.dim_date
                RESTART IDENTITY CASCADE
                """
            )
    finally:
        conn.close()


def write_jdbc(df, table: str) -> None:
    (
        df.write.format("jdbc")
        .option("url", JDBC_POSTGRES_URL)
        .option("dbtable", table)
        .mode("append")
        .options(**POSTGRES_PROPERTIES)
        .save()
    )


def build_dimensions(source):
    parsed = source.withColumn(
        "sale_date_parsed",
        F.coalesce(
            F.to_date("sale_date", "M/d/yyyy"),
            F.to_date("sale_date", "MM/dd/yyyy"),
            F.to_date("sale_date"),
        ),
    )

    dim_customer = (
        parsed.select(
            F.col("sale_customer_id").alias("customer_id"),
            "customer_first_name",
            "customer_last_name",
            "customer_age",
            "customer_email",
            "customer_country",
            "customer_postal_code",
            "customer_pet_type",
            "customer_pet_name",
            "customer_pet_breed",
        )
        .dropDuplicates(["customer_id"])
        .withColumn(
            "customer_key",
            F.row_number().over(Window.orderBy("customer_id")),
        )
    )

    dim_seller = (
        parsed.select(
            F.col("sale_seller_id").alias("seller_id"),
            "seller_first_name",
            "seller_last_name",
            "seller_email",
            "seller_country",
            "seller_postal_code",
        )
        .dropDuplicates(["seller_id"])
        .withColumn("seller_key", F.row_number().over(Window.orderBy("seller_id")))
    )

    dim_product = (
        parsed.select(
            F.col("sale_product_id").alias("product_id"),
            "product_name",
            "product_category",
            "product_price",
            "product_brand",
            "product_material",
            "product_color",
            "product_size",
            "product_weight",
            "product_rating",
            "product_reviews",
            "pet_category",
        )
        .dropDuplicates(["product_id"])
        .withColumn("product_key", F.row_number().over(Window.orderBy("product_id")))
    )

    dim_store = (
        parsed.select(
            "store_name",
            "store_location",
            "store_city",
            "store_state",
            "store_country",
            "store_phone",
            "store_email",
        )
        .dropDuplicates(["store_name", "store_city", "store_country"])
        .withColumn("store_key", F.row_number().over(Window.orderBy("store_name", "store_city")))
    )

    dim_supplier = (
        parsed.select(
            "supplier_name",
            "supplier_contact",
            "supplier_email",
            "supplier_phone",
            "supplier_address",
            "supplier_city",
            "supplier_country",
        )
        .dropDuplicates(["supplier_name"])
        .withColumn(
            "supplier_key",
            F.row_number().over(Window.orderBy("supplier_name")),
        )
    )

    dim_date = (
        parsed.select("sale_date_parsed")
        .where(F.col("sale_date_parsed").isNotNull())
        .dropDuplicates(["sale_date_parsed"])
        .withColumn("date_key", F.date_format("sale_date_parsed", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("sale_date_parsed"))
        .withColumn("month", F.month("sale_date_parsed"))
        .withColumn("quarter", F.quarter("sale_date_parsed"))
        .withColumn("day_of_month", F.dayofmonth("sale_date_parsed"))
        .withColumn("day_of_week", F.dayofweek("sale_date_parsed"))
        .withColumnRenamed("sale_date_parsed", "sale_date")
    )

    return parsed, dim_customer, dim_seller, dim_product, dim_store, dim_supplier, dim_date


def build_fact_sales(parsed, dims):
    dim_customer, dim_seller, dim_product, dim_store, dim_supplier, dim_date = dims

    fact = (
        parsed.alias("s")
        .join(dim_customer.alias("c"), F.col("s.sale_customer_id") == F.col("c.customer_id"))
        .join(dim_seller.alias("sl"), F.col("s.sale_seller_id") == F.col("sl.seller_id"))
        .join(dim_product.alias("p"), F.col("s.sale_product_id") == F.col("p.product_id"))
        .join(
            dim_store.alias("st"),
            (F.col("s.store_name") == F.col("st.store_name"))
            & (F.col("s.store_city") == F.col("st.store_city"))
            & (F.col("s.store_country") == F.col("st.store_country")),
        )
        .join(dim_supplier.alias("su"), F.col("s.supplier_name") == F.col("su.supplier_name"))
        .join(
            dim_date.alias("d"),
            F.col("s.sale_date_parsed") == F.col("d.sale_date"),
        )
        .select(
            F.col("s.id").alias("sale_id"),
            F.col("c.customer_key"),
            F.col("sl.seller_key"),
            F.col("p.product_key"),
            F.col("st.store_key"),
            F.col("su.supplier_key"),
            F.col("d.date_key"),
            F.col("s.sale_quantity"),
            F.col("s.sale_total_price"),
            F.col("s.product_price"),
        )
    )
    return fact


def main() -> None:
    import os

    spark = build_spark()
    try:
        source = read_mock_data(spark)
        row_count = source.count()
        print(f"Read {row_count} rows from staging.mock_data")
        if row_count == 0:
            raise RuntimeError("staging.mock_data is empty. Run load_mock_data first.")

        truncate_dwh_tables()

        parsed, *dims = build_dimensions(source)
        dim_customer, dim_seller, dim_product, dim_store, dim_supplier, dim_date = dims
        fact_sales = build_fact_sales(parsed, dims)

        write_jdbc(
            dim_customer.select(
                "customer_key",
                "customer_id",
                "customer_first_name",
                "customer_last_name",
                "customer_age",
                "customer_email",
                "customer_country",
                "customer_postal_code",
                "customer_pet_type",
                "customer_pet_name",
                "customer_pet_breed",
            ),
            "dwh.dim_customer",
        )
        write_jdbc(
            dim_seller.select(
                "seller_key",
                "seller_id",
                "seller_first_name",
                "seller_last_name",
                "seller_email",
                "seller_country",
                "seller_postal_code",
            ),
            "dwh.dim_seller",
        )
        write_jdbc(
            dim_product.select(
                "product_key",
                "product_id",
                "product_name",
                "product_category",
                "product_price",
                "product_brand",
                "product_material",
                "product_color",
                "product_size",
                "product_weight",
                "product_rating",
                "product_reviews",
                "pet_category",
            ),
            "dwh.dim_product",
        )
        write_jdbc(
            dim_store.select(
                "store_key",
                "store_name",
                "store_location",
                "store_city",
                "store_state",
                "store_country",
                "store_phone",
                "store_email",
            ),
            "dwh.dim_store",
        )
        write_jdbc(
            dim_supplier.select(
                "supplier_key",
                "supplier_name",
                "supplier_contact",
                "supplier_email",
                "supplier_phone",
                "supplier_address",
                "supplier_city",
                "supplier_country",
            ),
            "dwh.dim_supplier",
        )
        write_jdbc(
            dim_date.select(
                "date_key",
                "sale_date",
                "year",
                "month",
                "quarter",
                "day_of_month",
                "day_of_week",
            ),
            "dwh.dim_date",
        )
        write_jdbc(fact_sales, "dwh.fact_sales")

        print("Star schema ETL completed successfully")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
