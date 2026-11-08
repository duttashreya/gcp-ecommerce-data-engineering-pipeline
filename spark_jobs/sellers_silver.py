from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, upper


PROJECT_ROOT = "/home/shreya/gcp-ecommerce-data-engineering-pipeline"

INPUT_PATH = f"{PROJECT_ROOT}/data/raw/olist_sellers_dataset.csv"
OUTPUT_PATH = f"{PROJECT_ROOT}/data/processed/silver/sellers"


spark = (
    SparkSession.builder
    .appName("SellersSilver")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("Reading sellers CSV...")

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
    .withColumn("seller_id", trim(col("seller_id")))
    .withColumn("seller_city", lower(trim(col("seller_city"))))
    .withColumn("seller_state", upper(trim(col("seller_state"))))
)


# Mandatory field validation
df = df.filter(col("seller_id").isNotNull())


# Remove duplicate seller IDs
df = df.dropDuplicates(["seller_id"])


silver_count = df.count()

print(f"Silver record count: {silver_count}")

print(
    "Null seller IDs:",
    df.filter(col("seller_id").isNull()).count()
)

print(
    "Duplicate seller IDs:",
    silver_count - df.select("seller_id").distinct().count()
)


print("Writing Silver Parquet...")

(
    df.write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)

print("Silver sellers pipeline completed successfully.")

spark.stop()
