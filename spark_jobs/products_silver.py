from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim


# --------------------------------------------------
# 1. Project paths
# --------------------------------------------------

PROJECT_ROOT = "/home/shreya/gcp-ecommerce-data-engineering-pipeline"

INPUT_PATH = f"{PROJECT_ROOT}/data/raw/olist_products_dataset.csv"
OUTPUT_PATH = f"{PROJECT_ROOT}/data/processed/silver/products"


# --------------------------------------------------
# 2. Create Spark session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("ProductsSilver")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 3. Read raw products CSV
# --------------------------------------------------

print("Reading products CSV...")

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
# 5. Rename source columns with spelling issues
# --------------------------------------------------

df = df.withColumnRenamed(
    "product_name_lenght",
    "product_name_length"
)

df = df.withColumnRenamed(
    "product_description_lenght",
    "product_description_length"
)


# --------------------------------------------------
# 6. Clean product ID
# --------------------------------------------------

df = df.withColumn(
    "product_id",
    trim(col("product_id"))
)


# --------------------------------------------------
# 7. Standardize product category
# --------------------------------------------------

df = df.withColumn(
    "product_category_name",
    lower(trim(col("product_category_name")))
)


# --------------------------------------------------
# 8. Data quality filtering
# --------------------------------------------------

# Product ID is mandatory.
df = df.filter(
    col("product_id").isNotNull()
)


# --------------------------------------------------
# 9. Validate numeric attributes
# --------------------------------------------------

# These attributes should never be negative.
df = df.filter(
    (col("product_name_length").isNull()) |
    (col("product_name_length") >= 0)
)

df = df.filter(
    (col("product_description_length").isNull()) |
    (col("product_description_length") >= 0)
)

df = df.filter(
    (col("product_photos_qty").isNull()) |
    (col("product_photos_qty") >= 0)
)

df = df.filter(
    (col("product_weight_g").isNull()) |
    (col("product_weight_g") >= 0)
)

df = df.filter(
    (col("product_length_cm").isNull()) |
    (col("product_length_cm") >= 0)
)

df = df.filter(
    (col("product_height_cm").isNull()) |
    (col("product_height_cm") >= 0)
)

df = df.filter(
    (col("product_width_cm").isNull()) |
    (col("product_width_cm") >= 0)
)


# --------------------------------------------------
# 10. Remove duplicate product IDs
# --------------------------------------------------

df = df.dropDuplicates(["product_id"])


# --------------------------------------------------
# 11. Data quality checks
# --------------------------------------------------

silver_count = df.count()

null_product_ids = df.filter(
    col("product_id").isNull()
).count()

duplicate_product_ids = (
    df.groupBy("product_id")
    .count()
    .filter(col("count") > 1)
    .count()
)


print(f"Silver record count: {silver_count}")
print(f"Null product IDs: {null_product_ids}")
print(f"Duplicate product IDs: {duplicate_product_ids}")


# --------------------------------------------------
# 12. Write Silver Parquet
# --------------------------------------------------

print("Writing Silver Parquet...")

(
    df.write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)


print("Silver products pipeline completed successfully.")


# --------------------------------------------------
# 13. Stop Spark
# --------------------------------------------------

spark.stop()