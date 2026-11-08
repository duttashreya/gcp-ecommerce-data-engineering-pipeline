import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def main(batch_date):

    spark = (
        SparkSession.builder
        .appName("IncrementalBatchSimulator")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    source_path = "data/processed/silver/orders"

    output_path = (
        "data/incremental/orders/"
        f"batch_date={batch_date}"
    )

    print("=" * 60)
    print("INCREMENTAL BATCH SIMULATOR")
    print("=" * 60)
    print(f"Source: {source_path}")
    print(f"Batch date: {batch_date}")

    # ------------------------------------------------------------
    # READ SILVER ORDERS
    # ------------------------------------------------------------

    orders = spark.read.parquet(source_path)

    total_source_orders = orders.count()

    print(f"Total source orders: {total_source_orders}")

    # ------------------------------------------------------------
    # FILTER BATCH
    # ------------------------------------------------------------

    batch_df = (
        orders
        .filter(
            col("order_purchase_date") == batch_date
        )
    )

    batch_count = batch_df.count()

    print(f"Orders selected for batch: {batch_count}")

    if batch_count == 0:
        raise ValueError(
            f"No orders found for batch date: {batch_date}"
        )

    # ------------------------------------------------------------
    # WRITE BATCH
    # ------------------------------------------------------------

    (
        batch_df
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    # ------------------------------------------------------------
    # VERIFY
    # ------------------------------------------------------------

    written_df = spark.read.parquet(output_path)

    written_count = written_df.count()

    if written_count != batch_count:
        raise ValueError(
            f"Batch write verification failed. "
            f"Expected {batch_count}, got {written_count}"
        )

    print("\nIncremental batch created successfully.")
    print(f"Output path: {output_path}")
    print(f"Batch row count: {written_count}")

    spark.stop()


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print(
            "Usage: python "
            "ingestion/create_incremental_batch.py "
            "<batch_date>"
        )
        sys.exit(1)

    batch_date = sys.argv[1]

    main(batch_date)