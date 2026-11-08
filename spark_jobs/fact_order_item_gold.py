from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    round as spark_round
)


# --------------------------------------------------
# Project paths
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]

SILVER_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "silver"
)

OUTPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "gold"
    / "fact_order_item"
)


# --------------------------------------------------
# Spark session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("FactOrderItemGold")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# Read Silver datasets
# --------------------------------------------------

print("Reading Silver datasets...")

order_items = spark.read.parquet(
    str(SILVER_PATH / "order_items")
)

orders = spark.read.parquet(
    str(SILVER_PATH / "orders")
)

customers = spark.read.parquet(
    str(SILVER_PATH / "customers")
)

products = spark.read.parquet(
    str(SILVER_PATH / "products")
)

sellers = spark.read.parquet(
    str(SILVER_PATH / "sellers")
)


print(f"Order items: {order_items.count()}")
print(f"Orders: {orders.count()}")
print(f"Customers: {customers.count()}")
print(f"Products: {products.count()}")
print(f"Sellers: {sellers.count()}")


# --------------------------------------------------
# Prepare lookup datasets
# --------------------------------------------------

orders_lookup = orders.select(
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_date"
)

customers_lookup = customers.select(
    "customer_id"
)

products_lookup = products.select(
    "product_id"
)

sellers_lookup = sellers.select(
    "seller_id"
)


# --------------------------------------------------
# Build fact table
# --------------------------------------------------

fact_order_item = (
    order_items.alias("oi")

    # Join order information
    .join(
        orders_lookup.alias("o"),
        col("oi.order_id") == col("o.order_id"),
        "inner"
    )

    # Join customer
    .join(
        customers_lookup.alias("c"),
        col("o.customer_id") == col("c.customer_id"),
        "inner"
    )

    # Join product
    .join(
        products_lookup.alias("p"),
        col("oi.product_id") == col("p.product_id"),
        "inner"
    )

    # Join seller
    .join(
        sellers_lookup.alias("s"),
        col("oi.seller_id") == col("s.seller_id"),
        "inner"
    )

    .select(
        col("oi.order_id"),
        col("oi.order_item_id"),
        col("o.customer_id"),
        col("oi.product_id"),
        col("oi.seller_id"),
        col("o.order_purchase_date"),
        col("o.order_status"),
        col("oi.shipping_limit_date"),
        col("oi.price"),
        col("oi.freight_value"),

        # Business metric
        spark_round(
            col("oi.price") + col("oi.freight_value"),
            2
        ).alias("total_item_value")
    )
)


# --------------------------------------------------
# Data quality checks
# --------------------------------------------------

fact_count = fact_order_item.count()

duplicate_fact_keys = (
    fact_order_item
    .groupBy("order_id", "order_item_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

null_order_ids = (
    fact_order_item
    .filter(col("order_id").isNull())
    .count()
)

null_customer_ids = (
    fact_order_item
    .filter(col("customer_id").isNull())
    .count()
)

null_product_ids = (
    fact_order_item
    .filter(col("product_id").isNull())
    .count()
)

null_seller_ids = (
    fact_order_item
    .filter(col("seller_id").isNull())
    .count()
)


print(f"Fact row count: {fact_count}")
print(f"Duplicate fact keys: {duplicate_fact_keys}")
print(f"Null order IDs: {null_order_ids}")
print(f"Null customer IDs: {null_customer_ids}")
print(f"Null product IDs: {null_product_ids}")
print(f"Null seller IDs: {null_seller_ids}")


# --------------------------------------------------
# Display sample
# --------------------------------------------------

print("\nFact table sample:")

fact_order_item.show(
    10,
    truncate=False
)


# --------------------------------------------------
# Write Gold fact table
# --------------------------------------------------

print("Writing Gold fact_order_item...")

(
    fact_order_item
    .write
    .mode("overwrite")
    .parquet(str(OUTPUT_PATH))
)


print("Gold fact_order_item pipeline completed successfully.")


# --------------------------------------------------
# Stop Spark
# --------------------------------------------------

spark.stop()