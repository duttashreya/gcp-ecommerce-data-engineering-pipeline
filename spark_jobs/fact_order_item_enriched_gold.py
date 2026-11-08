from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


PROJECT_ROOT = Path(__file__).resolve().parents[1]

FACT_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "gold"
    / "fact_order_item"
)

PAYMENT_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "gold"
    / "order_payment_summary"
)

OUTPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "gold"
    / "fact_order_item_enriched"
)


spark = (
    SparkSession.builder
    .appName("FactOrderItemEnrichedGold")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("Reading Gold fact_order_item...")

fact = spark.read.parquet(str(FACT_PATH))

print(f"Fact rows before enrichment: {fact.count()}")


print("Reading Gold order_payment_summary...")

payments = spark.read.parquet(str(PAYMENT_PATH))

print(f"Payment summary rows: {payments.count()}")


# Validate payment summary grain before joining
duplicate_payment_orders = (
    payments
    .groupBy("order_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicate_payment_orders > 0:
    raise ValueError(
        f"Payment summary contains {duplicate_payment_orders} duplicate order IDs."
    )


print("Joining payment summary to fact_order_item...")

enriched_fact = (
    fact
    .join(
        payments,
        on="order_id",
        how="left"
    )
)


fact_rows_after_join = enriched_fact.count()

duplicate_fact_keys = (
    enriched_fact
    .groupBy("order_id", "order_item_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

null_order_ids = (
    enriched_fact
    .filter(col("order_id").isNull())
    .count()
)


print(f"Fact rows after enrichment: {fact_rows_after_join}")
print(f"Duplicate fact keys: {duplicate_fact_keys}")
print(f"Null order IDs: {null_order_ids}")


# Verify that the item-level grain was preserved
if fact_rows_after_join != fact.count():
    raise ValueError(
        "Row count changed after payment enrichment. "
        "This indicates possible row multiplication."
    )

if duplicate_fact_keys > 0:
    raise ValueError(
        "Duplicate fact keys detected after payment enrichment."
    )

if null_order_ids > 0:
    raise ValueError(
        "Null order IDs detected after payment enrichment."
    )


print("\nEnriched fact sample:")

enriched_fact.select(
    "order_id",
    "order_item_id",
    "customer_id",
    "product_id",
    "seller_id",
    "order_purchase_date",
    "order_status",
    "price",
    "freight_value",
    "total_item_value",
    "total_payment_value",
    "payment_count",
    "primary_payment_type"
).show(
    10,
    truncate=False
)


print("Writing Gold fact_order_item_enriched...")

(
    enriched_fact
    .write
    .mode("overwrite")
    .parquet(str(OUTPUT_PATH))
)


print("\nGold fact_order_item_enriched pipeline completed successfully.")


spark.stop()
