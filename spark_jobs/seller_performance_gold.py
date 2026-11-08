from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    countDistinct,
    sum as spark_sum,
    round,
    col
)

spark = (
    SparkSession.builder
    .appName("SellerPerformanceGold")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Reading final Gold fact...")

fact = spark.read.parquet(
    "data/processed/gold/fact_order_item_final"
)

print(f"Fact rows: {fact.count()}")

print("Building seller performance mart...")

seller_performance = (
    fact
    .groupBy("seller_id")
    .agg(
        countDistinct("order_id").alias("order_count"),

        countDistinct(
            "customer_id"
        ).alias("customer_count"),

        countDistinct(
            "order_id",
            "order_item_id"
        ).alias("items_sold"),

        round(
            spark_sum("price"),
            2
        ).alias("gross_product_revenue"),

        round(
            spark_sum("freight_value"),
            2
        ).alias("freight_revenue"),

        round(
            spark_sum("total_item_value"),
            2
        ).alias("total_revenue"),

        round(
            spark_sum("total_item_value")
            / countDistinct(
                "order_id",
                "order_item_id"
            ),
            2
        ).alias("average_item_value")
    )
    .orderBy(
        col("total_revenue").desc()
    )
)

print(
    f"Seller rows: {seller_performance.count()}"
)

duplicate_sellers = (
    seller_performance
    .groupBy("seller_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

null_sellers = (
    seller_performance
    .filter(col("seller_id").isNull())
    .count()
)

print(
    f"Duplicate sellers: {duplicate_sellers}"
)

print(
    f"Null sellers: {null_sellers}"
)

print("\nSeller performance sample:")

seller_performance.show(
    20,
    truncate=False
)

print("Writing Gold seller_performance...")

(
    seller_performance
    .write
    .mode("overwrite")
    .parquet(
        "data/processed/gold/seller_performance"
    )
)

print(
    "Gold seller_performance pipeline completed successfully."
)

spark.stop()
