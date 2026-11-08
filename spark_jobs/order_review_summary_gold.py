from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    max,
    row_number,
    when
)
from pyspark.sql.window import Window


PROJECT_ROOT = Path(__file__).resolve().parents[1]

INPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "silver"
    / "reviews"
)

OUTPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "gold"
    / "order_review_summary"
)


spark = (
    SparkSession.builder
    .appName("OrderReviewSummaryGold")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("Reading Silver reviews...")

reviews = spark.read.parquet(str(INPUT_PATH))

print(f"Review rows: {reviews.count()}")


# Aggregate review information to order grain
review_aggregates = (
    reviews
    .groupBy("order_id")
    .agg(
        count("*").alias("review_count"),
        avg("review_score").alias("average_review_score"),
        max("review_creation_date").alias("latest_review_date"),
        max(
            when(
                col("review_comment_message").isNotNull()
                & (col("review_comment_message") != ""),
                1
            ).otherwise(0)
        ).alias("has_review_comment")
    )
)


# Get the review score associated with the latest review
window_spec = Window.partitionBy("order_id").orderBy(
    col("review_creation_date").desc_nulls_last()
)

latest_review = (
    reviews
    .withColumn(
        "rn",
        row_number().over(window_spec)
    )
    .filter(col("rn") == 1)
    .select(
        "order_id",
        col("review_score").alias("latest_review_score")
    )
)


order_review_summary = (
    review_aggregates
    .join(
        latest_review,
        on="order_id",
        how="left"
    )
)


row_count = order_review_summary.count()

duplicate_order_ids = (
    order_review_summary
    .groupBy("order_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

null_order_ids = (
    order_review_summary
    .filter(col("order_id").isNull())
    .count()
)


print(f"Review summary order count: {row_count}")
print(f"Duplicate order IDs: {duplicate_order_ids}")
print(f"Null order IDs: {null_order_ids}")


print("\nReview summary sample:")

order_review_summary.show(
    10,
    truncate=False
)


print("Writing Gold order_review_summary...")

(
    order_review_summary
    .write
    .mode("overwrite")
    .parquet(str(OUTPUT_PATH))
)


print("\nGold order_review_summary pipeline completed successfully.")


spark.stop()
