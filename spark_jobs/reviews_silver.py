from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lower,
    trim,
    to_date,
    to_timestamp
)


# --------------------------------------------------
# Project paths
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]

INPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "olist_order_reviews_dataset.csv"
)

OUTPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "silver"
    / "reviews"
)


# --------------------------------------------------
# Spark session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("ReviewsSilverPipeline")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# Read raw CSV
# --------------------------------------------------

print("Reading reviews CSV...")

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("quote", '"')
    .option("escape", '"')
    .option("multiLine", "true")
    .csv(str(INPUT_PATH))
)

source_count = df.count()

print(f"Source record count: {source_count}")


# --------------------------------------------------
# Standardize column names
# --------------------------------------------------

df = df.toDF(
    *[
        c.strip().lower().replace(" ", "_")
        for c in df.columns
    ]
)


# --------------------------------------------------
# Clean string columns
# --------------------------------------------------

df = (
    df
    .withColumn("review_id", trim(col("review_id")))
    .withColumn("order_id", trim(col("order_id")))
    .withColumn("review_comment_title", trim(col("review_comment_title")))
    .withColumn("review_comment_message", trim(col("review_comment_message")))
)


# --------------------------------------------------
# Standardize review score
# --------------------------------------------------

df = df.withColumn(
    "review_score",
    col("review_score").cast("integer")
)


# --------------------------------------------------
# Standardize dates/timestamps
# --------------------------------------------------

df = (
    df
    .withColumn(
        "review_creation_date",
        to_timestamp(col("review_creation_date"))
    )
    .withColumn(
        "review_answer_timestamp",
        to_timestamp(col("review_answer_timestamp"))
    )
)


# --------------------------------------------------
# Standardize text values
# --------------------------------------------------

df = (
    df
    .withColumn(
        "review_comment_title",
        lower(col("review_comment_title"))
    )
    .withColumn(
        "review_comment_message",
        lower(col("review_comment_message"))
    )
)


# --------------------------------------------------
# Data quality rules
# --------------------------------------------------

# review_id is mandatory
df = df.filter(
    col("review_id").isNotNull()
)

# order_id is mandatory for a valid review record
df = df.filter(
    col("order_id").isNotNull()
)

# Review score must be between 1 and 5 when present
df = df.filter(
    col("review_score").isNull()
    | col("review_score").between(1, 5)
)


# --------------------------------------------------
# Remove duplicate review records
# --------------------------------------------------

df = df.dropDuplicates(
    [
        "review_id",
        "order_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_answer_timestamp",
    ]
)


# --------------------------------------------------
# Quality checks
# --------------------------------------------------

silver_count = df.count()

null_review_ids = df.filter(
    col("review_id").isNull()
).count()

null_order_ids = df.filter(
    col("order_id").isNull()
).count()

invalid_scores = df.filter(
    col("review_score").isNotNull()
    & ~col("review_score").between(1, 5)
).count()


print(f"Silver record count: {silver_count}")
print(f"Null review IDs: {null_review_ids}")
print(f"Null order IDs: {null_order_ids}")
print(f"Invalid review scores: {invalid_scores}")


# --------------------------------------------------
# Write Silver Parquet
# --------------------------------------------------

print("Writing Silver Parquet...")

(
    df.write
    .mode("overwrite")
    .parquet(str(OUTPUT_PATH))
)

print("Silver reviews pipeline completed successfully.")


# --------------------------------------------------
# Stop Spark
# --------------------------------------------------

spark.stop()