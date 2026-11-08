from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    upper
)


# --------------------------------------------------
# Project paths
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]

INPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "silver"
    / "customers"
)

OUTPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "gold"
    / "dim_customer"
)


# --------------------------------------------------
# Spark session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("DimCustomerGold")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# Read Silver customers
# --------------------------------------------------

print("Reading Silver customers...")

df = spark.read.parquet(str(INPUT_PATH))

print(f"Silver customer count: {df.count()}")


# --------------------------------------------------
# Build dimension
# --------------------------------------------------

dim_customer = (
    df
    .select(
        col("customer_id"),
        col("customer_unique_id"),
        col("customer_zip_code_prefix").alias("zip_code_prefix"),
        lower(trim(col("customer_city"))).alias("city"),
        upper(trim(col("customer_state"))).alias("state")
    )
    .dropDuplicates(["customer_id"])
)


# --------------------------------------------------
# Data quality checks
# --------------------------------------------------

row_count = dim_customer.count()

duplicate_customer_ids = (
    dim_customer
    .groupBy("customer_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

null_customer_ids = (
    dim_customer
    .filter(col("customer_id").isNull())
    .count()
)


print(f"Gold customer count: {row_count}")
print(f"Duplicate customer IDs: {duplicate_customer_ids}")
print(f"Null customer IDs: {null_customer_ids}")


# --------------------------------------------------
# Write Gold dimension
# --------------------------------------------------

print("Writing Gold dim_customer...")

(
    dim_customer
    .write
    .mode("overwrite")
    .parquet(str(OUTPUT_PATH))
)


print("Gold dim_customer pipeline completed successfully.")


# --------------------------------------------------
# Stop Spark
# --------------------------------------------------

spark.stop()