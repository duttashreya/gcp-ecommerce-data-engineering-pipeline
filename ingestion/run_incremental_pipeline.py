import os
import sys
import subprocess
from datetime import datetime


PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)


def run_step(step_name, command):
    """
    Run one pipeline step and stop the pipeline if it fails.
    """

    print("\n" + "=" * 70)
    print(f"STEP: {step_name}")
    print("=" * 70)

    print("Command:")
    print(" ".join(command))

    result = subprocess.run(
        command,
        cwd=PROJECT_ROOT
    )

    if result.returncode != 0:
        print("\n" + "=" * 70)
        print(f"FAILED: {step_name}")
        print("=" * 70)

        raise RuntimeError(
            f"Pipeline stopped because '{step_name}' failed."
        )

    print("\n" + "-" * 70)
    print(f"SUCCESS: {step_name}")
    print("-" * 70)


def main():

    if len(sys.argv) != 2:
        print(
            "Usage: python "
            "ingestion/run_incremental_pipeline.py "
            "<batch_date>"
        )
        sys.exit(1)

    batch_date = sys.argv[1]

    # Validate date format before starting the pipeline.
    try:
        datetime.strptime(batch_date, "%Y-%m-%d")
    except ValueError:
        print(
            f"Invalid batch date: {batch_date}. "
            "Expected format: YYYY-MM-DD"
        )
        sys.exit(1)

    python_executable = sys.executable

    print("\n" + "=" * 70)
    print("INCREMENTAL E-COMMERCE DATA PIPELINE")
    print("=" * 70)
    print(f"Batch date: {batch_date}")
    print(f"Python: {python_executable}")
    print(f"Project root: {PROJECT_ROOT}")

    # ------------------------------------------------------------
    # STEP 1 — CREATE INCREMENTAL BATCH
    # ------------------------------------------------------------

    run_step(
        "Create Incremental Batch",
        [
            python_executable,
            "ingestion/create_incremental_batch.py",
            batch_date
        ]
    )

    # ------------------------------------------------------------
    # STEP 2 — INCREMENTAL SILVER
    # ------------------------------------------------------------

    run_step(
        "Incremental Orders Silver",
        [
            python_executable,
            "ingestion/incremental_orders_silver.py",
            batch_date
        ]
    )

    # ------------------------------------------------------------
    # STEP 3 — INCREMENTAL FACT GOLD
    # ------------------------------------------------------------

    run_step(
        "Incremental Fact Order Item Gold",
        [
            python_executable,
            "spark_jobs/incremental_fact_order_item_gold.py",
            batch_date
        ]
    )

    # ------------------------------------------------------------
    # STEP 4 — INCREMENTAL SALES DAILY GOLD
    # ------------------------------------------------------------

    run_step(
        "Incremental Sales Daily Gold",
        [
            python_executable,
            "spark_jobs/incremental_sales_daily_gold.py",
            batch_date
        ]
    )

    # ------------------------------------------------------------
    # PIPELINE COMPLETE
    # ------------------------------------------------------------

    print("\n" + "=" * 70)
    print("INCREMENTAL PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 70)

    print(f"Batch date: {batch_date}")
    print("Status: PASSED")


if __name__ == "__main__":
    main()
