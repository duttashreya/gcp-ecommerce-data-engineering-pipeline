import os
import sys

PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

from datetime import datetime

from tests.audit_logger import (
    create_incremental_audit_record,
    write_incremental_audit_record,
)

from tests.incremental_data_quality import (
    validate_incremental_orders_batch,
)

OUTPUT_PATH = "data/processed/silver_incremental/orders"


def main():

    # =========================================================
    # 1. Validate command-line argument
    # =========================================================

    if len(sys.argv) != 2:
        print(
            "Usage: python ingestion/incremental_orders_silver.py "
            "<batch_date>"
        )
        sys.exit(1)

    batch_date = sys.argv[1]

    input_path = (
        f"data/incremental/orders/batch_date={batch_date}"
    )

    spark = (
        SparkSession.builder
        .appName("IncrementalOrdersSilver")
        .master("local[*]")
        .getOrCreate()
    )

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 60)
    print("IDEMPOTENT INCREMENTAL ORDERS SILVER PIPELINE")
    print("=" * 60)

    print(f"Run ID: {run_id}")
    print(f"Batch date: {batch_date}")
    print(f"Input: {input_path}")
    print(f"Output: {OUTPUT_PATH}")

    # Audit variables
    incoming_count = 0
    existing_count = 0
    combined_count = 0
    duplicates_removed = 0
    final_count = 0

    try:

        # =====================================================
        # 2. Read incoming batch
        # =====================================================

        if not os.path.exists(input_path):
            raise FileNotFoundError(
                f"Batch does not exist: {input_path}"
            )

        incoming = spark.read.parquet(input_path)

        incoming_count = incoming.count()

        print()
        print(f"Incoming batch rows: {incoming_count}")

        # =====================================================
        # 3. Incremental Data Quality Validation
        # =====================================================

        print()
        print("=" * 60)
        print("INCREMENTAL DATA QUALITY VALIDATION")
        print("=" * 60)

        dq_results, dq_status = validate_incremental_orders_batch(
            incoming
        )

        for result in dq_results:
            print(
                f"{result['check']}: "
                f"{result['status']} - "
                f"{result['details']}"
            )

        print()
        print(f"Incremental DQ status: {dq_status}")

        if dq_status == "FAIL":
            raise ValueError(
                "Incremental data quality validation failed."
            )

        # =====================================================
        # 4. Validate incoming batch
        # =====================================================

        null_order_ids = (
            incoming
            .filter(col("order_id").isNull())
            .count()
        )

        duplicate_order_ids = (
            incoming
            .groupBy("order_id")
            .count()
            .filter(col("count") > 1)
            .count()
        )

        print(f"Null order IDs: {null_order_ids}")
        print(
            f"Duplicate order IDs in batch: "
            f"{duplicate_order_ids}"
        )

        if null_order_ids > 0:
            raise ValueError(
                "Incoming batch contains NULL order IDs."
            )

        if duplicate_order_ids > 0:
            raise ValueError(
                "Incoming batch contains duplicate order IDs."
            )

        # =====================================================
        # 5. Read existing Silver data
        # =====================================================

        if os.path.exists(OUTPUT_PATH):

            existing = spark.read.parquet(OUTPUT_PATH)

            existing_count = existing.count()

            print(
                f"Existing Silver rows: "
                f"{existing_count}"
            )

        else:

            existing = None
            existing_count = 0

            print(
                "No existing incremental Silver data found."
            )

        # =====================================================
        # 6. Combine existing + incoming
        # =====================================================

        if existing is not None:

            combined = existing.unionByName(incoming)

        else:

            combined = incoming

        combined_count = combined.count()

        print(
            f"Combined rows before deduplication: "
            f"{combined_count}"
        )

        # =====================================================
        # 7. Deduplicate by order_id
        # =====================================================

        window_spec = (
            Window
            .partitionBy("order_id")
            .orderBy(
                col("order_purchase_timestamp").desc()
            )
        )

        deduplicated = (
            combined
            .withColumn(
                "row_number",
                row_number().over(window_spec)
            )
            .filter(
                col("row_number") == 1
            )
            .drop("row_number")
        )

        final_count = deduplicated.count()

        duplicates_removed = (
            combined_count - final_count
        )

        print(
            f"Final Silver rows: {final_count}"
        )

        print(
            f"Duplicate rows removed: "
            f"{duplicates_removed}"
        )

        # =====================================================
        # 8. Write consolidated Silver
        # =====================================================

        (
            deduplicated
            .write
            .mode("overwrite")
            .parquet(OUTPUT_PATH)
        )

        print()
        print(
            "Incremental Silver merge completed successfully."
        )

        print(f"Output: {OUTPUT_PATH}")
        print(f"Final row count: {final_count}")

        # =====================================================
        # 9. Write SUCCESS audit record
        # =====================================================

        audit_record = create_incremental_audit_record(
            run_id=run_id,
            pipeline_name="incremental_orders_silver",
            batch_date=batch_date,
            dataset_name="orders",
            input_row_count=incoming_count,
            existing_row_count=existing_count,
            combined_row_count=combined_count,
            duplicates_removed=duplicates_removed,
            final_row_count=final_count,
            status="PASS",
            error_message=None,
        )

        write_incremental_audit_record(
            spark,
            audit_record
        )

        print()
        print("Incremental pipeline status: PASS")

    except Exception as e:

        # =====================================================
        # 10. Write FAILURE audit record
        # =====================================================

        error_message = str(e)

        print()
        print("Incremental pipeline failed.")
        print(f"Error: {error_message}")

        audit_record = create_incremental_audit_record(
            run_id=run_id,
            pipeline_name="incremental_orders_silver",
            batch_date=batch_date,
            dataset_name="orders",
            input_row_count=incoming_count,
            existing_row_count=existing_count,
            combined_row_count=combined_count,
            duplicates_removed=duplicates_removed,
            final_row_count=final_count,
            status="FAIL",
            error_message=error_message,
        )

        write_incremental_audit_record(
            spark,
            audit_record
        )

        print()
        print("Incremental pipeline status: FAIL")

        spark.stop()

        sys.exit(1)

    spark.stop()


if __name__ == "__main__":
    main()