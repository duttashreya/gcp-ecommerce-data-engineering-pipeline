from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    count,
    row_number
)
from pyspark.sql.window import Window


# --------------------------------------------------
# Project paths
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]

INPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "silver"
    / "payments"
)

OUTPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "gold"
    / "order_payment_summary"
)


# --------------------------------------------------
# Spark session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("OrderPaymentSummaryGold")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# Read Silver payments
# --------------------------------------------------

print("Reading Silver payments...")

payments = spark.read.parquet(str(INPUT_PATH))

print(f"Payment rows: {payments.count()}")


# --------------------------------------------------
# Total payment value per order
# --------------------------------------------------

payment_totals = (
    payments
    .groupBy("order_id")
    .agg(
        spark_sum("payment_value").alias("total_payment_value"),
        count("*").alias("payment_count")
    )
)


# --------------------------------------------------
# Determine primary payment type
# --------------------------------------------------

window_spec = Window.partitionBy("order_id").orderBy(
    col("payment_value").desc(),
    col("payment_sequential")
)

primary_payment = (
    payments
    .withColumn(
        "rn",
        row_number().over(window_spec)
    )
    .filter(col("rn") == 1)
    .select(
        "order_id",
        col("payment_type").alias("primary_payment_type")
    )
)


# --------------------------------------------------
# Combine order-level payment summary
# --------------------------------------------------

order_payment_summary = (
    payment_totals
    .join(
        primary_payment,
        on="order_id",
        how="left"
    )
)


# --------------------------------------------------
# Data quality checks
# --------------------------------------------------

row_count = order_payment_summary.count()

duplicate_order_ids = (
    order_payment_summary
    .groupBy("order_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

null_order_ids = (
    order_payment_summary
    .filter(col("order_id").isNull())
    .count()
)

print(f"Payment summary order count: {row_count}")
print(f"Duplicate order IDs: {duplicate_order_ids}")
print(f"Null order IDs: {null_order_ids}")


# --------------------------------------------------
# Display sample
# --------------------------------------------------

print("\nPayment summary sample:")

order_payment_summary.show(
    10,
    truncate=False
)


# --------------------------------------------------
# Write Gold payment summary
# --------------------------------------------------

print("Writing Gold order_payment_summary...")

(
    order_payment_summary
    .write
    .mode("overwrite")
    .parquet(str(OUTPUT_PATH))
)


print("Gold order_payment_summary pipeline completed successfully.")


# --------------------------------------------------
# Stop Spark
# --------------------------------------------------

spark.stop()