from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    countDistinct,
    round,
    sum as spark_sum
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]

INPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "gold"
    / "fact_order_item_final"
)

OUTPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "gold"
    / "sales_daily"
)


spark = (
    SparkSession.builder
    .appName("SalesDailyGold")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("Reading final Gold fact...")

fact = spark.read.parquet(str(INPUT_PATH))

fact_count = fact.count()

print(f"Fact rows: {fact_count}")


print("Building daily sales mart...")


sales_daily = (
    fact
    .groupBy("order_purchase_date")
    .agg(
        countDistinct("order_id").alias("order_count"),

        countDistinct("customer_id").alias("customer_count"),

        countDistinct(
            "order_id",
            "order_item_id"
        ).alias("items_sold"),

        round(
            spark_sum("price"),
            2
        ).alias("gross_product_revenue"),

        round(
            spark_sum("freight_value"),
            2
        ).alias("freight_revenue"),

        round(
            spark_sum("total_item_value"),
            2
        ).alias("total_revenue"),

        round(
            spark_sum("total_item_value")
            / countDistinct("order_id"),
            2
        ).alias("average_order_value")
    )
    .orderBy("order_purchase_date")
)


row_count = sales_daily.count()


duplicate_dates = (
    sales_daily
    .groupBy("order_purchase_date")
    .count()
    .filter("count > 1")
    .count()
)


null_dates = (
    sales_daily
    .filter(
        sales_daily.order_purchase_date.isNull()
    )
    .count()
)


print(f"Sales daily rows: {row_count}")
print(f"Duplicate dates: {duplicate_dates}")
print(f"Null purchase dates: {null_dates}")


print("\nSales daily sample:")

sales_daily.show(
    10,
    truncate=False
)


print("Writing Gold sales_daily...")


(
    sales_daily
    .write
    .mode("overwrite")
    .parquet(str(OUTPUT_PATH))
)


print("\nGold sales_daily pipeline completed successfully.")


spark.stop()
