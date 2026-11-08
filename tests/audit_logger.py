from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
)
from datetime import datetime
import os


AUDIT_PATH = "data/audit/pipeline_audit"


# ============================================================
# Standard Data Quality Audit Schema
# ============================================================

def create_audit_schema():

    return StructType([
        StructField("run_id", StringType(), False),
        StructField("pipeline_name", StringType(), False),
        StructField("run_timestamp", StringType(), False),
        StructField("dataset_name", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("row_count", LongType(), True),
        StructField("status", StringType(), False),
        StructField("error_message", StringType(), True),
    ])


def create_audit_record(
    run_id,
    pipeline_name,
    dataset_name,
    layer,
    row_count,
    status,
    error_message=None,
):

    return {
        "run_id": run_id,
        "pipeline_name": pipeline_name,
        "run_timestamp": datetime.now().isoformat(),
        "dataset_name": dataset_name,
        "layer": layer,
        "row_count": row_count,
        "status": status,
        "error_message": error_message,
    }


# ============================================================
# Incremental Processing Audit Schema
# ============================================================

def create_incremental_audit_schema():

    return StructType([
        StructField("run_id", StringType(), False),
        StructField("pipeline_name", StringType(), False),
        StructField("run_timestamp", StringType(), False),
        StructField("batch_date", StringType(), False),
        StructField("dataset_name", StringType(), False),
        StructField("input_row_count", LongType(), True),
        StructField("existing_row_count", LongType(), True),
        StructField("combined_row_count", LongType(), True),
        StructField("duplicates_removed", LongType(), True),
        StructField("final_row_count", LongType(), True),
        StructField("status", StringType(), False),
        StructField("error_message", StringType(), True),
    ])


def create_incremental_audit_record(
    run_id,
    pipeline_name,
    batch_date,
    dataset_name,
    input_row_count,
    existing_row_count,
    combined_row_count,
    duplicates_removed,
    final_row_count,
    status,
    error_message=None,
):

    return {
        "run_id": run_id,
        "pipeline_name": pipeline_name,
        "run_timestamp": datetime.now().isoformat(),
        "batch_date": batch_date,
        "dataset_name": dataset_name,
        "input_row_count": input_row_count,
        "existing_row_count": existing_row_count,
        "combined_row_count": combined_row_count,
        "duplicates_removed": duplicates_removed,
        "final_row_count": final_row_count,
        "status": status,
        "error_message": error_message,
    }


# ============================================================
# Standard Audit Writer
# ============================================================

def write_audit_records(spark, records):

    if not records:
        return

    schema = create_audit_schema()

    audit_df = spark.createDataFrame(
        records,
        schema=schema,
    )

    audit_df.write.mode("append").parquet(AUDIT_PATH)

    print()
    print("Audit records written successfully.")
    print(f"Audit path: {AUDIT_PATH}")
    print(f"Audit records: {len(records)}")


# ============================================================
# Incremental Audit Writer
# ============================================================

def write_incremental_audit_record(spark, record):

    schema = create_incremental_audit_schema()

    audit_df = spark.createDataFrame(
        [record],
        schema=schema,
    )

    audit_df.write.mode("append").parquet(
        "data/audit/incremental_pipeline_audit"
    )

    print()
    print("Incremental audit record written successfully.")
    print(
        "Audit path: "
        "data/audit/incremental_pipeline_audit"
    )


# ============================================================
# Read Standard Audit Log
# ============================================================

def read_audit_log(spark):

    if not os.path.exists(AUDIT_PATH):
        return None

    return spark.read.parquet(AUDIT_PATH)