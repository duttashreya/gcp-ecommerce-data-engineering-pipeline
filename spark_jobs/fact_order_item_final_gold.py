from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


PROJECT_ROOT = Path(__file__).resolve().parents[1]

FACT_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "gold"
    / "fact_order_item_enriched"
)

REVIEW_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "gold"
    / "order_review_summary"
)

OUTPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "gold"
    / "fact_order_item_final"
)


spark = (
    SparkSession.builder
    .appName("FactOrderItemFinalGold")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("Reading enriched fact...")

fact = spark.read.parquet(str(FACT_PATH))

fact_count_before = fact.count()

print(f"Fact rows before review enrichment: {fact_count_before}")


print("Reading Gold order_review_summary...")

reviews = spark.read.parquet(str(REVIEW_PATH))

review_count = reviews.count()

print(f"Review summary rows: {review_count}")


# Validate review summary grain
duplicate_review_orders = (
    reviews
    .groupBy("order_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicate_review_orders > 0:
    raise ValueError(
        f"Review summary contains {duplicate_review_orders} duplicate order IDs."
    )


print("Joining review summary to enriched fact...")


final_fact = (
    fact
    .join(
        reviews,
        on="order_id",
        how="left"
    )
)


fact_count_after = final_fact.count()

duplicate_fact_keys = (
    final_fact
    .groupBy("order_id", "order_item_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

null_order_ids = (
    final_fact
    .filter(col("order_id").isNull())
    .count()
)


print(f"Fact rows after review enrichment: {fact_count_after}")
print(f"Duplicate fact keys: {duplicate_fact_keys}")
print(f"Null order IDs: {null_order_ids}")


# Critical grain validation
if fact_count_after != fact_count_before:
    raise ValueError(
        "Fact row count changed after review enrichment. "
        "This indicates possible row multiplication."
    )

if duplicate_fact_keys > 0:
    raise ValueError(
        "Duplicate fact keys detected after review enrichment."
    )

if null_order_ids > 0:
    raise ValueError(
        "Null order IDs detected after review enrichment."
    )


print("\nFinal fact sample:")

final_fact.select(
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
    "primary_payment_type",
    "review_count",
    "average_review_score",
    "latest_review_score",
    "latest_review_date",
    "has_review_comment"
).show(
    10,
    truncate=False
)


print("Writing Gold fact_order_item_final...")


(
    final_fact
    .write
    .mode("overwrite")
    .parquet(str(OUTPUT_PATH))
)


print("\nGold fact_order_item_final pipeline completed successfully.")


spark.stop()
