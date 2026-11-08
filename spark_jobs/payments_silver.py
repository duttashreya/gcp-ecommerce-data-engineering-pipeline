from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim


PROJECT_ROOT = "/home/shreya/gcp-ecommerce-data-engineering-pipeline"

INPUT_PATH = f"{PROJECT_ROOT}/data/raw/olist_order_payments_dataset.csv"
OUTPUT_PATH = f"{PROJECT_ROOT}/data/processed/silver/payments"


spark = (
    SparkSession.builder
    .appName("PaymentsSilver")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("Reading payments CSV...")

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(INPUT_PATH)
)

source_count = df.count()
print(f"Source record count: {source_count}")


# Standardize column names
for old_name in df.columns:
    new_name = (
        old_name.strip()
        .lower()
        .replace(" ", "_")
    )

    if old_name != new_name:
        df = df.withColumnRenamed(old_name, new_name)


# Standardize string columns
df = (
    df
    .withColumn("order_id", trim(col("order_id")))
    .withColumn("payment_type", lower(trim(col("payment_type"))))
)


# Mandatory field validation
df = df.filter(
    col("order_id").isNotNull()
    & col("payment_type").isNotNull()
)


# Numeric validation
df = df.filter(
    col("payment_sequential") > 0
)

df = df.filter(
    col("payment_value") >= 0
)


# Remove duplicate payment records
df = df.dropDuplicates(
    ["order_id", "payment_sequential"]
)


silver_count = df.count()

print(f"Silver record count: {silver_count}")

print(
    "Null order IDs:",
    df.filter(col("order_id").isNull()).count()
)

print(
    "Duplicate payment keys:",
    silver_count
    - df.select("order_id", "payment_sequential").distinct().count()
)


print("Writing Silver Parquet...")

(
    df.write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)

print("Silver payments pipeline completed successfully.")

spark.stop()
