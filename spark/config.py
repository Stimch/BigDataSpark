import os

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "bigdata")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "lab")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "lab")

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = os.environ.get("CLICKHOUSE_PORT", "8123")
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "lab")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "lab")

JDBC_POSTGRES_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)
JDBC_CLICKHOUSE_URL = (
    f"jdbc:ch://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}"
)

POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

CLICKHOUSE_PROPERTIES = {
    "user": CLICKHOUSE_USER,
    "password": CLICKHOUSE_PASSWORD,
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
}

SPARK_JARS = os.environ.get(
    "SPARK_JARS",
    "/opt/spark/jars-extra/postgresql-42.7.3.jar,"
    "/opt/spark/jars-extra/clickhouse-jdbc-0.6.5-all.jar",
)
