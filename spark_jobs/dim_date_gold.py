from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    year,
    quarter,
    month,
    monthname,
    weekofyear,
    dayofmonth,
    dayofweek,
    date_format,
    when,
    lit,
    min,
    max,
    sequence,
    explode
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
    / "orders"
)

OUTPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "gold"
    / "dim_date"
)


# --------------------------------------------------
# Spark session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("DimDateGold")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# Read Silver orders
# --------------------------------------------------

print("Reading Silver orders...")

orders = spark.read.parquet(str(INPUT_PATH))


# --------------------------------------------------
# Determine date range
# --------------------------------------------------

date_range = orders.select(
    min("order_purchase_date").alias("min_date"),
    max("order_purchase_date").alias("max_date")
).collect()[0]

min_date = date_range["min_date"]
max_date = date_range["max_date"]

print(f"Minimum order date: {min_date}")
print(f"Maximum order date: {max_date}")


# --------------------------------------------------
# Generate continuous date range
# --------------------------------------------------

dates = (
    spark.range(1)
    .select(
        explode(
            sequence(
                lit(min_date),
                lit(max_date)
            )
        ).alias("full_date")
    )
)


# --------------------------------------------------
# Build Date Dimension
# --------------------------------------------------

dim_date = (
    dates
    .withColumn(
        "date_key",
        date_format(col("full_date"), "yyyyMMdd").cast("integer")
    )
    .withColumn("year", year(col("full_date")))
    .withColumn("quarter", quarter(col("full_date")))
    .withColumn("month", month(col("full_date")))
    .withColumn("month_name", monthname(col("full_date")))
    .withColumn("week_of_year", weekofyear(col("full_date")))
    .withColumn("day_of_month", dayofmonth(col("full_date")))
    .withColumn("day_of_week", dayofweek(col("full_date")))
    .withColumn(
        "day_name",
        date_format(col("full_date"), "EEEE")
    )
    .withColumn(
        "is_weekend",
        when(
            dayofweek(col("full_date")).isin(1, 7),
            lit(True)
        ).otherwise(lit(False))
    )
    .select(
        "date_key",
        "full_date",
        "year",
        "quarter",
        "month",
        "month_name",
        "week_of_year",
        "day_of_month",
        "day_of_week",
        "day_name",
        "is_weekend"
    )
    .orderBy("full_date")
)


# --------------------------------------------------
# Data quality checks
# --------------------------------------------------

gold_count = dim_date.count()

null_date_keys = (
    dim_date
    .filter(col("date_key").isNull())
    .count()
)

duplicate_date_keys = (
    dim_date
    .groupBy("date_key")
    .count()
    .filter(col("count") > 1)
    .count()
)

print(f"Gold date count: {gold_count}")
print(f"Duplicate date keys: {duplicate_date_keys}")
print(f"Null date keys: {null_date_keys}")


# --------------------------------------------------
# Display sample
# --------------------------------------------------

print("\nDate dimension sample:")

dim_date.show(10, truncate=False)


# --------------------------------------------------
# Write Gold dimension
# --------------------------------------------------

print("Writing Gold dim_date...")

(
    dim_date
    .write
    .mode("overwrite")
    .parquet(str(OUTPUT_PATH))
)


print("Gold dim_date pipeline completed successfully.")


# --------------------------------------------------
# Stop Spark
# --------------------------------------------------

spark.stop()