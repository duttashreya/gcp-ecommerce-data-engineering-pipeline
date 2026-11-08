from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    sum as spark_sum,
    avg,
    round,
    when,
    col,
    datediff
)

spark = (
    SparkSession.builder
    .appName("DeliveryPerformanceGold")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Reading Silver orders...")

orders = spark.read.parquet(
    "data/processed/silver/orders"
)

print(f"Order rows: {orders.count()}")

print("Building delivery performance mart...")

delivery = (
    orders
    .withColumn(
        "delivery_days",
        datediff(
            col("order_delivered_customer_date"),
            col("order_purchase_timestamp")
        )
    )
    .withColumn(
        "estimated_delivery_days",
        datediff(
            col("order_estimated_delivery_date"),
            col("order_purchase_timestamp")
        )
    )
    .withColumn(
        "delivery_delay_days",
        datediff(
            col("order_delivered_customer_date"),
            col("order_estimated_delivery_date")
        )
    )
)

delivery_performance = (
    delivery
    .agg(
        count("*").alias("total_orders"),

        spark_sum(
            when(
                col("order_delivered_customer_date").isNotNull(),
                1
            ).otherwise(0)
        ).alias("delivered_orders"),

        round(
            avg(
                when(
                    col("order_delivered_customer_date").isNotNull(),
                    col("delivery_days")
                )
            ),
            2
        ).alias("average_delivery_days"),

        round(
            avg(
                when(
                    col("order_delivered_customer_date").isNotNull(),
                    col("estimated_delivery_days")
                )
            ),
            2
        ).alias("average_estimated_delivery_days"),

        round(
            avg(
                when(
                    col("order_delivered_customer_date").isNotNull(),
                    col("delivery_delay_days")
                )
            ),
            2
        ).alias("average_delivery_delay_days"),

        round(
            100.0 *
            spark_sum(
                when(
                    (
                        col("order_delivered_customer_date").isNotNull()
                    ) &
                    (
                        col("delivery_delay_days") <= 0
                    ),
                    1
                ).otherwise(0)
            )
            /
            spark_sum(
                when(
                    col("order_delivered_customer_date").isNotNull(),
                    1
                ).otherwise(0)
            ),
            2
        ).alias("on_time_delivery_rate_pct"),

        round(
            100.0 *
            spark_sum(
                when(
                    (
                        col("order_delivered_customer_date").isNotNull()
                    ) &
                    (
                        col("delivery_delay_days") > 0
                    ),
                    1
                ).otherwise(0)
            )
            /
            spark_sum(
                when(
                    col("order_delivered_customer_date").isNotNull(),
                    1
                ).otherwise(0)
            ),
            2
        ).alias("late_delivery_rate_pct")
    )
)

print("\nDelivery performance:")

delivery_performance.show(
    truncate=False
)

print("Writing Gold delivery_performance...")

(
    delivery_performance
    .write
    .mode("overwrite")
    .parquet(
        "data/processed/gold/delivery_performance"
    )
)

print(
    "Gold delivery_performance pipeline completed successfully."
)

spark.stop()
