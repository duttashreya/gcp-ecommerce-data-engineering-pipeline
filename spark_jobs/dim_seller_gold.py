from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, upper


# --------------------------------------------------
# Project paths
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]

INPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "silver"
    / "sellers"
)

OUTPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "gold"
    / "dim_seller"
)


# --------------------------------------------------
# Spark session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("DimSellerGold")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# Read Silver sellers
# --------------------------------------------------

print("Reading Silver sellers...")

df = spark.read.parquet(str(INPUT_PATH))

silver_count = df.count()

print(f"Silver seller count: {silver_count}")


# --------------------------------------------------
# Build dimension
# --------------------------------------------------

dim_seller = (
    df
    .select(
        col("seller_id"),
        col("seller_zip_code_prefix").alias("zip_code_prefix"),
        lower(trim(col("seller_city"))).alias("city"),
        upper(trim(col("seller_state"))).alias("state")
    )
    .dropDuplicates(["seller_id"])
)


# --------------------------------------------------
# Data quality checks
# --------------------------------------------------

gold_count = dim_seller.count()

duplicate_seller_ids = (
    dim_seller
    .groupBy("seller_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

null_seller_ids = (
    dim_seller
    .filter(col("seller_id").isNull())
    .count()
)


print(f"Gold seller count: {gold_count}")
print(f"Duplicate seller IDs: {duplicate_seller_ids}")
print(f"Null seller IDs: {null_seller_ids}")


# --------------------------------------------------
# Write Gold dimension
# --------------------------------------------------

print("Writing Gold dim_seller...")

(
    dim_seller
    .write
    .mode("overwrite")
    .parquet(str(OUTPUT_PATH))
)


print("Gold dim_seller pipeline completed successfully.")


# --------------------------------------------------
# Stop Spark
# --------------------------------------------------

spark.stop()