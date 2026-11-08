from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    trim,
    lower
)


# ---------------------------------------------------------
# PROJECT PATHS
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]

INPUT_PATH = str(
    PROJECT_ROOT / "data" / "raw" / "olist_orders_dataset.csv"
)

OUTPUT_PATH = str(
    PROJECT_ROOT / "data" / "processed" / "silver" / "orders"
)


# ---------------------------------------------------------
# CREATE SPARK SESSION
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("OlistOrdersSilver")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ---------------------------------------------------------
# READ SOURCE DATA
# ---------------------------------------------------------

print("Reading orders CSV...")

orders = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(INPUT_PATH)
)

print("Source record count:", orders.count())


# ---------------------------------------------------------
# STANDARDIZE COLUMN NAMES
# ---------------------------------------------------------

for old_column in orders.columns:

    new_column = (
        old_column
        .strip()
        .lower()
        .replace(" ", "_")
    )

    orders = orders.withColumnRenamed(
        old_column,
        new_column
    )


# ---------------------------------------------------------
# CONVERT DATE/TIME COLUMNS
# ---------------------------------------------------------

timestamp_columns = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

for column_name in timestamp_columns:

    orders = orders.withColumn(
        column_name,
        to_timestamp(col(column_name))
    )


# ---------------------------------------------------------
# STANDARDIZE ORDER STATUS
# ---------------------------------------------------------

orders = orders.withColumn(
    "order_status",
    lower(trim(col("order_status")))
)


# ---------------------------------------------------------
# DATA QUALITY FILTER
# ---------------------------------------------------------

orders = orders.filter(
    col("order_id").isNotNull()
)

orders = orders.filter(
    col("order_purchase_timestamp").isNotNull()
)


# ---------------------------------------------------------
# DEDUPLICATION
# ---------------------------------------------------------

orders = orders.dropDuplicates(
    ["order_id"]
)


# ---------------------------------------------------------
# ADD DERIVED DATE
# ---------------------------------------------------------

orders = orders.withColumn(
    "order_purchase_date",
    to_date(col("order_purchase_timestamp"))
)


# ---------------------------------------------------------
# DATA QUALITY CHECK
# ---------------------------------------------------------

print(
    "Silver record count:",
    orders.count()
)

print(
    "Null order IDs:",
    orders.filter(
        col("order_id").isNull()
    ).count()
)

print(
    "Duplicate order IDs:",
    orders.groupBy("order_id")
    .count()
    .filter(col("count") > 1)
    .count()
)


# ---------------------------------------------------------
# WRITE SILVER DATA
# ---------------------------------------------------------

print("Writing Silver Parquet...")

(
    orders.write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)


print("Silver orders pipeline completed successfully.")


# ---------------------------------------------------------
# STOP SPARK
# ---------------------------------------------------------

spark.stop()