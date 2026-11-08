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
    .appName("CustomerPerformanceGold")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Reading final Gold fact...")

fact = spark.read.parquet(
    "data/processed/gold/fact_order_item_final"
)

print(f"Fact rows: {fact.count()}")

print("Reading order review summary...")

reviews = spark.read.parquet(
    "data/processed/gold/order_review_summary"
)

print(f"Review summary rows: {reviews.count()}")

print("Building customer revenue metrics...")

customer_revenue = (
    fact
    .groupBy("customer_id")
    .agg(
        countDistinct("order_id").alias("order_count"),

        countDistinct(
            "order_id",
            "order_item_id"
        ).alias("items_purchased"),

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
            / countDistinct("order_id"),
            2
        ).alias("average_order_value")
    )
)

print("Building customer review metrics...")

customer_reviews = (
    fact
    .select(
        "customer_id",
        "order_id"
    )
    .dropDuplicates()
    .join(
        reviews,
        on="order_id",
        how="left"
    )
    .groupBy("customer_id")
    .agg(
        round(
            avg("average_review_score"),
            2
        ).alias("average_review_score"),

        round(
            avg("review_count"),
            2
        ).alias("average_reviews_per_order"),

        countDistinct(
            col("order_id")
        )
        .alias("reviewed_orders")
    )
)

print("Joining customer revenue and review metrics...")

customer_performance = (
    customer_revenue
    .join(
        customer_reviews,
        on="customer_id",
        how="left"
    )
    .select(
        "customer_id",
        "order_count",
        "items_purchased",
        "gross_product_revenue",
        "freight_revenue",
        "total_revenue",
        "average_order_value",
        "average_review_score",
        "average_reviews_per_order",
        "reviewed_orders"
    )
    .orderBy(
        col("total_revenue").desc()
    )
)

print(
    f"Customer rows: {customer_performance.count()}"
)

duplicate_customers = (
    customer_performance
    .groupBy("customer_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

null_customers = (
    customer_performance
    .filter(col("customer_id").isNull())
    .count()
)

print(
    f"Duplicate customers: {duplicate_customers}"
)

print(
    f"Null customers: {null_customers}"
)

print("\nCustomer performance sample:")

customer_performance.show(
    20,
    truncate=False
)

print("Writing Gold customer_performance...")

(
    customer_performance
    .write
    .mode("overwrite")
    .parquet(
        "data/processed/gold/customer_performance"
    )
)

print(
    "Gold customer_performance pipeline completed successfully."
)

spark.stop()