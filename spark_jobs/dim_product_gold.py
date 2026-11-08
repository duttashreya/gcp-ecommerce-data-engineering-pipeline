from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    lower
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
    / "products"
)

OUTPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "gold"
    / "dim_product"
)


# --------------------------------------------------
# Spark session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("DimProductGold")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# Read Silver products
# --------------------------------------------------

print("Reading Silver products...")

df = spark.read.parquet(str(INPUT_PATH))

silver_count = df.count()

print(f"Silver product count: {silver_count}")


# --------------------------------------------------
# Build dimension
# --------------------------------------------------

dim_product = (
    df
    .select(
        col("product_id"),
        lower(trim(col("product_category_name"))).alias(
            "product_category"
        ),
        col("product_name_length"),
        col("product_description_length"),
        col("product_photos_qty"),
        col("product_weight_g"),
        col("product_length_cm"),
        col("product_height_cm"),
        col("product_width_cm")
    )
    .dropDuplicates(["product_id"])
)


# --------------------------------------------------
# Data quality checks
# --------------------------------------------------

gold_count = dim_product.count()

duplicate_product_ids = (
    dim_product
    .groupBy("product_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

null_product_ids = (
    dim_product
    .filter(col("product_id").isNull())
    .count()
)

null_categories = (
    dim_product
    .filter(col("product_category").isNull())
    .count()
)


print(f"Gold product count: {gold_count}")
print(f"Duplicate product IDs: {duplicate_product_ids}")
print(f"Null product IDs: {null_product_ids}")
print(f"Null product categories: {null_categories}")


# --------------------------------------------------
# Write Gold dimension
# --------------------------------------------------

print("Writing Gold dim_product...")

(
    dim_product
    .write
    .mode("overwrite")
    .parquet(str(OUTPUT_PATH))
)


print("Gold dim_product pipeline completed successfully.")


# --------------------------------------------------
# Stop Spark
# --------------------------------------------------

spark.stop()