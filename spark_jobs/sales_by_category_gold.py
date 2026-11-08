from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    countDistinct,
    sum as spark_sum,
    round,
    col,
    coalesce,
    lit
)

spark = (
    SparkSession.builder
    .appName("SalesByCategoryGold")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Reading final Gold fact...")
fact = spark.read.parquet(
    "data/processed/gold/fact_order_item_final"
)

print(f"Fact rows: {fact.count()}")

print("Reading product dimension...")
dim_product = spark.read.parquet(
    "data/processed/gold/dim_product"
).select(
    "product_id",
    col("product_category").alias("product_category_name")
)

print(f"Product dimension rows: {dim_product.count()}")

print("Joining fact with product dimension...")

fact_with_category = (
    fact
    .join(
        dim_product,
        on="product_id",
        how="left"
    )
)

print(f"Rows after product join: {fact_with_category.count()}")

print("Building sales by category mart...")

sales_by_category = (
    fact_with_category
    .withColumn(
        "product_category_name",
        coalesce(
            col("product_category_name"),
            lit("unknown")
        )
    )
    .groupBy("product_category_name")
    .agg(
        countDistinct("order_id").alias("order_count"),

        countDistinct(
            "order_id",
            "order_item_id"
        ).alias("items_sold"),

        countDistinct(
            "customer_id"
        ).alias("customer_count"),

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
    f"Category rows: {sales_by_category.count()}"
)

duplicate_categories = (
    sales_by_category
    .groupBy("product_category_name")
    .count()
    .filter(col("count") > 1)
    .count()
)

null_categories = (
    sales_by_category
    .filter(
        col("product_category_name").isNull()
    )
    .count()
)

print(
    f"Duplicate categories: {duplicate_categories}"
)

print(
    f"Null categories: {null_categories}"
)

print("\nSales by category sample:")

sales_by_category.show(
    20,
    truncate=False
)

print("Writing Gold sales_by_category...")

(
    sales_by_category
    .write
    .mode("overwrite")
    .parquet(
        "data/processed/gold/sales_by_category"
    )
)

print(
    "Gold sales_by_category pipeline completed successfully."
)

spark.stop()
