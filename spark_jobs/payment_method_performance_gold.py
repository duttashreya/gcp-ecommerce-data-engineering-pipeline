from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    countDistinct,
    sum as spark_sum,
    avg,
    round,
    col
)

spark = (
    SparkSession.builder
    .appName("PaymentMethodPerformanceGold")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Reading Silver payments...")

payments = spark.read.parquet(
    "data/processed/silver/payments"
)

print(f"Payment rows: {payments.count()}")

print("Building payment method performance mart...")

payment_method_performance = (
    payments
    .groupBy("payment_type")
    .agg(
        countDistinct("order_id").alias("order_count"),

        countDistinct(
            "order_id"
        ).alias("customer_order_count"),

        spark_sum(
            "payment_value"
        ).alias("total_payment_value"),

        round(
            avg("payment_value"),
            2
        ).alias("average_payment_value"),

        round(
            avg("payment_installments"),
            2
        ).alias("average_installments"),

        spark_sum(
            col("payment_installments")
        ).alias("total_installments")
    )
    .select(
        "payment_type",
        "order_count",
        "customer_order_count",
        round(
            col("total_payment_value"),
            2
        ).alias("total_payment_value"),
        "average_payment_value",
        "average_installments",
        "total_installments"
    )
    .orderBy(
        col("total_payment_value").desc()
    )
)

print(
    f"Payment method rows: {payment_method_performance.count()}"
)

duplicate_payment_types = (
    payment_method_performance
    .groupBy("payment_type")
    .count()
    .filter(col("count") > 1)
    .count()
)

null_payment_types = (
    payment_method_performance
    .filter(col("payment_type").isNull())
    .count()
)

print(
    f"Duplicate payment types: {duplicate_payment_types}"
)

print(
    f"Null payment types: {null_payment_types}"
)

print("\nPayment method performance sample:")

payment_method_performance.show(
    20,
    truncate=False
)

print("Writing Gold payment_method_performance...")

(
    payment_method_performance
    .write
    .mode("overwrite")
    .parquet(
        "data/processed/gold/payment_method_performance"
    )
)

print(
    "Gold payment_method_performance pipeline completed successfully."
)

spark.stop()
