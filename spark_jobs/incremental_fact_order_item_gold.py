from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round
import os
import sys


OUTPUT_PATH = "data/processed/gold_incremental/fact_order_item"


def main():

    if len(sys.argv) != 2:
        print(
            "Usage: python spark_jobs/incremental_fact_order_item_gold.py "
            "<batch_date>"
        )
        sys.exit(1)

    batch_date = sys.argv[1]

    spark = (
        SparkSession.builder
        .appName("IncrementalFactOrderItemGold")
        .master("local[*]")
        .getOrCreate()
    )

    print("=" * 70)
    print("INCREMENTAL FACT ORDER ITEM GOLD PIPELINE")
    print("=" * 70)

    print(f"Batch date: {batch_date}")
    print(f"Output: {OUTPUT_PATH}")

    # =========================================================
    # 1. Read incremental orders
    # =========================================================

    incremental_orders_path = (
        "data/processed/silver_incremental/orders"
    )

    orders = (
        spark.read.parquet(incremental_orders_path)
        .filter(
            col("order_purchase_date") == batch_date
        )
    )

    order_count = orders.count()

    print()
    print(f"Incremental orders: {order_count}")

    if order_count == 0:
        raise ValueError(
            f"No orders found for batch date {batch_date}"
        )

    # =========================================================
    # 2. Read Silver dimensions/facts
    # =========================================================

    order_items = spark.read.parquet(
        "data/processed/silver/order_items"
    )

    customers = spark.read.parquet(
        "data/processed/silver/customers"
    )

    products = spark.read.parquet(
        "data/processed/silver/products"
    )

    sellers = spark.read.parquet(
        "data/processed/silver/sellers"
    )

    # =========================================================
    # 3. Join orders → order items
    # =========================================================

    fact = (
        orders
        .join(
            order_items,
            on="order_id",
            how="inner"
        )
    )

    print(
        f"Order-item rows after order join: "
        f"{fact.count()}"
    )

    # =========================================================
    # 4. Join customer
    # =========================================================

    fact = (
        fact
        .join(
            customers.select(
                "customer_id"
            ),
            on="customer_id",
            how="left"
        )
    )

    # =========================================================
    # 5. Join product
    # =========================================================

    fact = (
        fact
        .join(
            products.select(
                "product_id"
            ),
            on="product_id",
            how="left"
        )
    )

    # =========================================================
    # 6. Join seller
    # =========================================================

    fact = (
        fact
        .join(
            sellers.select(
                "seller_id"
            ),
            on="seller_id",
            how="left"
        )
    )

    # =========================================================
    # 7. Select final fact columns
    # =========================================================

    fact = (
        fact
        .select(
            "order_id",
            "order_item_id",
            "customer_id",
            "product_id",
            "seller_id",
            "order_purchase_date",
            "order_status",
            "shipping_limit_date",
            "price",
            "freight_value",
            spark_round(
                col("price") + col("freight_value"),
                2
            ).alias("total_item_value"),
        )
    )

    # =========================================================
    # 8. Validate fact grain
    # =========================================================

    duplicate_fact_keys = (
        fact
        .groupBy(
            "order_id",
            "order_item_id"
        )
        .count()
        .filter(col("count") > 1)
        .count()
    )

    null_order_ids = (
        fact
        .filter(col("order_id").isNull())
        .count()
    )

    null_customer_ids = (
        fact
        .filter(col("customer_id").isNull())
        .count()
    )

    null_product_ids = (
        fact
        .filter(col("product_id").isNull())
        .count()
    )

    null_seller_ids = (
        fact
        .filter(col("seller_id").isNull())
        .count()
    )

    negative_total_values = (
        fact
        .filter(
            col("total_item_value") < 0
        )
        .count()
    )

    final_count = fact.count()

    print()
    print("FACT QUALITY CHECKS")
    print("-" * 70)
    print(
        f"Final fact rows: {final_count}"
    )
    print(
        f"Duplicate fact keys: {duplicate_fact_keys}"
    )
    print(
        f"Null order IDs: {null_order_ids}"
    )
    print(
        f"Null customer IDs: {null_customer_ids}"
    )
    print(
        f"Null product IDs: {null_product_ids}"
    )
    print(
        f"Null seller IDs: {null_seller_ids}"
    )
    print(
        f"Negative total item values: "
        f"{negative_total_values}"
    )

    if duplicate_fact_keys > 0:
        raise ValueError(
            "Duplicate fact keys detected."
        )

    if null_order_ids > 0:
        raise ValueError(
            "NULL order IDs detected."
        )

    if null_customer_ids > 0:
        raise ValueError(
            "NULL customer IDs detected."
        )

    if null_product_ids > 0:
        raise ValueError(
            "NULL product IDs detected."
        )

    if null_seller_ids > 0:
        raise ValueError(
            "NULL seller IDs detected."
        )

    if negative_total_values > 0:
        raise ValueError(
            "Negative total item values detected."
        )

    # =========================================================
    # 9. Write incremental Gold
    # =========================================================

    batch_output_path = (
        f"{OUTPUT_PATH}/batch_date={batch_date}"
    )

    (
        fact
        .write
        .mode("overwrite")
        .parquet(batch_output_path)
    )

    print()
    print(
        "Incremental fact Gold pipeline completed successfully."
    )
    print(
        f"Output: {batch_output_path}"
    )
    print(
        f"Final fact rows: {final_count}"
    )

    spark.stop()


if __name__ == "__main__":
    main()
