from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim


# --------------------------------------------------
# 1. Project paths
# --------------------------------------------------

PROJECT_ROOT = "/home/shreya/gcp-ecommerce-data-engineering-pipeline"

INPUT_PATH = f"{PROJECT_ROOT}/data/raw/olist_order_items_dataset.csv"
OUTPUT_PATH = f"{PROJECT_ROOT}/data/processed/silver/order_items"


# --------------------------------------------------
# 2. Create Spark session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("OrderItemsSilver")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 3. Read raw order items CSV
# --------------------------------------------------

print("Reading order items CSV...")

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(INPUT_PATH)
)

source_count = df.count()

print(f"Source record count: {source_count}")


# --------------------------------------------------
# 4. Standardize column names
# --------------------------------------------------

df = df.toDF(
    *[
        column.strip().lower().replace(" ", "_")
        for column in df.columns
    ]
)


# --------------------------------------------------
# 5. Clean string columns
# --------------------------------------------------

string_columns = [
    "order_id",
    "product_id",
    "seller_id"
]

for column_name in string_columns:
    df = df.withColumn(
        column_name,
        trim(col(column_name))
    )


# --------------------------------------------------
# 6. Data quality filtering
# --------------------------------------------------

df = df.filter(
    col("order_id").isNotNull()
)

df = df.filter(
    col("order_item_id").isNotNull()
)

df = df.filter(
    col("product_id").isNotNull()
)

df = df.filter(
    col("seller_id").isNotNull()
)


# --------------------------------------------------
# 7. Validate numeric values
# --------------------------------------------------

df = df.filter(
    col("price") >= 0
)

df = df.filter(
    col("freight_value") >= 0
)


# --------------------------------------------------
# 8. Remove duplicate item records
# --------------------------------------------------

df = df.dropDuplicates(
    ["order_id", "order_item_id"]
)


# --------------------------------------------------
# 9. Add data quality checks
# --------------------------------------------------

silver_count = df.count()

null_order_ids = df.filter(
    col("order_id").isNull()
).count()

duplicate_item_keys = (
    df.groupBy("order_id", "order_item_id")
    .count()
    .filter(col("count") > 1)
    .count()
)


print(f"Silver record count: {silver_count}")
print(f"Null order IDs: {null_order_ids}")
print(f"Duplicate order item keys: {duplicate_item_keys}")


# --------------------------------------------------
# 10. Write Silver Parquet
# --------------------------------------------------

print("Writing Silver Parquet...")

(
    df.write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)


print("Silver order items pipeline completed successfully.")


# --------------------------------------------------
# 11. Stop Spark
# --------------------------------------------------

spark.stop()
