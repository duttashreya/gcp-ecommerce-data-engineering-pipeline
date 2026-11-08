import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    round,
    sum as spark_sum,
    when
)

PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def main(batch_date):

    spark = (
        SparkSession.builder
        .appName("IncrementalSalesDailyGold")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    input_path = (
        "data/processed/gold_incremental/"
        "fact_order_item"
        f"/batch_date={batch_date}"
    )

    output_path = (
        "data/processed/gold_incremental/"
        "sales_daily"
        f"/batch_date={batch_date}"
    )

    print("=" * 70)
    print("INCREMENTAL SALES DAILY GOLD PIPELINE")
    print("=" * 70)
    print(f"Batch date: {batch_date}")
    print(f"Input: {input_path}")
    print(f"Output: {output_path}")

    # ------------------------------------------------------------
    # READ INCREMENTAL FACT
    # ------------------------------------------------------------

    fact_df = spark.read.parquet(input_path)

    fact_count = fact_df.count()

    print(f"\nInput fact rows: {fact_count}")

    if fact_count == 0:
        raise ValueError("Incremental fact contains zero rows.")

    # ------------------------------------------------------------
    # BUILD DAILY SALES SUMMARY
    # ------------------------------------------------------------

    sales_daily = (
        fact_df
        .groupBy("order_purchase_date")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            count("*").alias("total_items"),
            round(
                spark_sum("total_item_value"),
                2
            ).alias("total_revenue")
        )
        .withColumn(
            "average_order_value",
            round(
                col("total_revenue") /
                when(
                    col("total_orders") > 0,
                    col("total_orders")
                ),
                2
            )
        )
        .select(
            col("order_purchase_date").alias("sales_date"),
            "total_orders",
            "total_items",
            "total_revenue",
            "average_order_value"
        )
    )

    # ------------------------------------------------------------
    # QUALITY CHECKS
    # ------------------------------------------------------------

    print("\nSALES DAILY QUALITY CHECKS")
    print("-" * 70)

    result = sales_daily.collect()

    if len(result) != 1:
        raise ValueError(
            f"Expected exactly 1 sales date, found {len(result)}"
        )

    row = result[0]

    print(f"Sales date: {row['sales_date']}")
    print(f"Total orders: {row['total_orders']}")
    print(f"Total items: {row['total_items']}")
    print(f"Total revenue: {row['total_revenue']}")
    print(f"Average order value: {row['average_order_value']}")

    duplicate_dates = (
        sales_daily
        .groupBy("sales_date")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    negative_revenue = (
        sales_daily
        .filter(col("total_revenue") < 0)
        .count()
    )

    null_sales_dates = (
        sales_daily
        .filter(col("sales_date").isNull())
        .count()
    )

    print(f"Duplicate sales dates: {duplicate_dates}")
    print(f"Negative revenue rows: {negative_revenue}")
    print(f"Null sales dates: {null_sales_dates}")

    if duplicate_dates != 0:
        raise ValueError("Duplicate sales dates detected.")

    if negative_revenue != 0:
        raise ValueError("Negative revenue detected.")

    if null_sales_dates != 0:
        raise ValueError("Null sales date detected.")

    # ------------------------------------------------------------
    # WRITE
    # ------------------------------------------------------------

    (
        sales_daily
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    print("\nIncremental sales_daily Gold pipeline completed successfully.")
    print(f"Output: {output_path}")

    spark.stop()


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print(
            "Usage: python "
            "spark_jobs/incremental_sales_daily_gold.py "
            "<batch_date>"
        )
        sys.exit(1)

    main(sys.argv[1])
