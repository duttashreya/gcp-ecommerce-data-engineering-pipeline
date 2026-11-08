from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, upper


# --------------------------------------------------
# 1. Project paths
# --------------------------------------------------

PROJECT_ROOT = "/home/shreya/gcp-ecommerce-data-engineering-pipeline"

INPUT_PATH = f"{PROJECT_ROOT}/data/raw/olist_customers_dataset.csv"
OUTPUT_PATH = f"{PROJECT_ROOT}/data/processed/silver/customers"


# --------------------------------------------------
# 2. Create Spark session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("CustomersSilver")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 3. Read raw customers CSV
# --------------------------------------------------

print("Reading customers CSV...")

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
    "customer_id",
    "customer_unique_id",
    "customer_city",
    "customer_state"
]

for column_name in string_columns:
    df = df.withColumn(
        column_name,
        trim(col(column_name))
    )


# --------------------------------------------------
# 6. Standardize city and state
# --------------------------------------------------

df = df.withColumn(
    "customer_city",
    lower(col("customer_city"))
)

df = df.withColumn(
    "customer_state",
    upper(trim(col("customer_state")))
)


# --------------------------------------------------
# 7. Data quality filtering
# --------------------------------------------------

df = df.filter(
    col("customer_id").isNotNull()
)

df = df.filter(
    col("customer_unique_id").isNotNull()
)


# --------------------------------------------------
# 8. Remove duplicate customer IDs
# --------------------------------------------------

df = df.dropDuplicates(["customer_id"])


# --------------------------------------------------
# 9. Add data quality checks
# --------------------------------------------------

silver_count = df.count()

null_customer_ids = df.filter(
    col("customer_id").isNull()
).count()

duplicate_customer_ids = (
    df.groupBy("customer_id")
    .count()
    .filter(col("count") > 1)
    .count()
)


print(f"Silver record count: {silver_count}")
print(f"Null customer IDs: {null_customer_ids}")
print(f"Duplicate customer IDs: {duplicate_customer_ids}")


# --------------------------------------------------
# 10. Write Silver Parquet
# --------------------------------------------------

print("Writing Silver Parquet...")

(
    df.write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)


print("Silver customers pipeline completed successfully.")


# --------------------------------------------------
# 11. Stop Spark
# --------------------------------------------------

spark.stop()
