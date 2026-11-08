from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    countDistinct,
    sum as spark_sum,
    round,
    col
)

spark = (
    SparkSession.builder
    .appName("ProductPerformanceGold")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Reading final Gold fact...")

fact = spark.read.parquet(
    "data/processed/gold/fact_order_item_final"
)

print(f"Fact rows: {fact.count()}")

print("Reading product dimension...")

products = spark.read.parquet(
    "data/processed/gold/dim_product"
).select(
    "product_id",
    col("product_category").alias("product_category_name")
)

print(f"Product dimension rows: {products.count()}")

print("Joining fact with product dimension...")

product_fact = (
    fact
    .join(
        products,
        on="product_id",
        how="left"
    )
)

print(
    f"Rows after product join: {product_fact.count()}"
)

print("Building product performance mart...")

product_performance = (
    product_fact
    .groupBy(
        "product_id",
        "product_category_name"
    )
    .agg(
        countDistinct(
            "order_id"
        ).alias("order_count"),

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
            /
            countDistinct(
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
    f"Product rows: {product_performance.count()}"
)

duplicate_products = (
    product_performance
    .groupBy("product_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

null_product_ids = (
    product_performance
    .filter(col("product_id").isNull())
    .count()
)

null_categories = (
    product_performance
    .filter(col("product_category_name").isNull())
    .count()
)

print(
    f"Duplicate products: {duplicate_products}"
)

print(
    f"Null product IDs: {null_product_ids}"
)

print(
    f"Null product categories: {null_categories}"
)

print("\nProduct performance sample:")

product_performance.show(
    20,
    truncate=False
)

print("Writing Gold product_performance...")

(
    product_performance
    .write
    .mode("overwrite")
    .parquet(
        "data/processed/gold/product_performance"
    )
)

print(
    "Gold product_performance pipeline completed successfully."
)

spark.stop()
