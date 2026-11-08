from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import sys
import os
from datetime import datetime

# Add project root to Python path
PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)

sys.path.insert(0, PROJECT_ROOT)

from tests.data_quality_config import DATA_QUALITY_CONFIG

from tests.audit_logger import (
    create_audit_record,
    write_audit_records,
)


# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("EcommerceDataQualityFramework")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# Global Configuration
# ============================================================

quality_failed = False

run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

pipeline_name = "ecommerce_data_quality_pipeline"

audit_records = []


# ============================================================
# Helper Functions
# ============================================================

def print_check(check_name, passed, details=""):

    global quality_failed

    status = "PASS" if passed else "FAIL"

    print(f"  {check_name:<35}: {status}")

    if details:
        print(f"      {details}")

    if not passed:
        quality_failed = True


def check_row_count(df, min_rows):

    actual_count = df.count()

    return (
        actual_count >= min_rows,
        actual_count,
        f"Minimum expected: {min_rows}, Actual: {actual_count}"
    )


def check_required_columns(df, required_columns):

    actual_columns = set(df.columns)

    missing_columns = [
        column
        for column in required_columns
        if column not in actual_columns
    ]

    passed = len(missing_columns) == 0

    details = (
        "All required columns present"
        if passed
        else f"Missing columns: {missing_columns}"
    )

    return passed, details


def check_nulls(df, column_name):

    null_count = (
        df
        .filter(col(column_name).isNull())
        .count()
    )

    return (
        null_count == 0,
        f"Null count: {null_count}"
    )


def check_duplicates(df, primary_key):

    duplicate_groups = (
        df
        .groupBy(*primary_key)
        .count()
        .filter(col("count") > 1)
        .count()
    )

    return (
        duplicate_groups == 0,
        f"Duplicate key groups: {duplicate_groups}"
    )


# ============================================================
# Dataset Validation
# ============================================================

def validate_dataset(dataset_name, config):

    global quality_failed

    print()
    print(f"[{dataset_name}]")

    path = config["path"]

    print(f"  Path: {path}")

    # --------------------------------------------------------
    # Read dataset
    # --------------------------------------------------------

    df = spark.read.parquet(path)

    # --------------------------------------------------------
    # Row count
    # --------------------------------------------------------

    passed, row_count, details = check_row_count(
        df,
        config["min_rows"]
    )

    print_check(
        "Row count",
        passed,
        details
    )

    # --------------------------------------------------------
    # Required columns
    # --------------------------------------------------------

    passed, details = check_required_columns(
        df,
        config["required_columns"]
    )

    print_check(
        "Required columns",
        passed,
        details
    )

    # --------------------------------------------------------
    # Primary key checks
    # --------------------------------------------------------

    primary_key = config["primary_key"]

    print(
        f"  Primary key: {', '.join(primary_key)}"
    )

    for column_name in primary_key:

        passed, details = check_nulls(
            df,
            column_name
        )

        print_check(
            f"Null {column_name}",
            passed,
            details
        )

    # --------------------------------------------------------
    # Primary key uniqueness
    # --------------------------------------------------------

    passed, details = check_duplicates(
        df,
        primary_key
    )

    print_check(
        "Primary key uniqueness",
        passed,
        details
    )

    # --------------------------------------------------------
    # Layer
    # --------------------------------------------------------

    layer = (
        "gold"
        if "/gold/" in path
        else "silver"
    )

    # --------------------------------------------------------
    # Dataset-level audit record
    # --------------------------------------------------------

    audit_records.append(
        create_audit_record(
            run_id=run_id,
            pipeline_name=pipeline_name,
            dataset_name=dataset_name,
            layer=layer,
            row_count=row_count,
            status="PASS" if passed else "FAIL",
            error_message=None,
        )
    )

    return df


# ============================================================
# MAIN
# ============================================================

print()
print("============================================================")
print("       CONFIGURATION-DRIVEN DATA QUALITY FRAMEWORK")
print("============================================================")

print(f"Run ID: {run_id}")
print(f"Pipeline: {pipeline_name}")


datasets = {}

for dataset_name, config in DATA_QUALITY_CONFIG.items():

    datasets[dataset_name] = validate_dataset(
        dataset_name,
        config
    )


# ============================================================
# BUSINESS QUALITY CHECKS
# ============================================================

print()
print("============================================================")
print("             BUSINESS QUALITY CHECKS")
print("============================================================")


# ------------------------------------------------------------
# Order Items
# ------------------------------------------------------------

if "order_items" in datasets:

    order_items = datasets["order_items"]

    negative_price_count = (
        order_items
        .filter(col("price") < 0)
        .count()
    )

    print_check(
        "Order item price >= 0",
        negative_price_count == 0,
        f"Negative price records: {negative_price_count}"
    )


# ------------------------------------------------------------
# Payments
# ------------------------------------------------------------

if "payments" in datasets:

    payments = datasets["payments"]

    negative_payment_count = (
        payments
        .filter(col("payment_value") < 0)
        .count()
    )

    print_check(
        "Payment value >= 0",
        negative_payment_count == 0,
        f"Negative payment records: {negative_payment_count}"
    )


# ------------------------------------------------------------
# Reviews
# ------------------------------------------------------------

if "reviews" in datasets:

    reviews = datasets["reviews"]

    invalid_review_scores = (
        reviews
        .filter(
            (col("review_score") < 1) |
            (col("review_score") > 5)
        )
        .count()
    )

    print_check(
        "Review score between 1 and 5",
        invalid_review_scores == 0,
        f"Invalid review scores: {invalid_review_scores}"
    )


# ------------------------------------------------------------
# Fact Revenue
# ------------------------------------------------------------

if "fact_order_item_final" in datasets:

    fact = datasets["fact_order_item_final"]

    negative_revenue_count = (
        fact
        .filter(col("total_item_value") < 0)
        .count()
    )

    print_check(
        "Fact total_item_value >= 0",
        negative_revenue_count == 0,
        f"Negative revenue records: {negative_revenue_count}"
    )


# ============================================================
# REFERENTIAL INTEGRITY
# ============================================================

print()
print("============================================================")
print("             REFERENTIAL INTEGRITY CHECKS")
print("============================================================")


if "fact_order_item_final" in datasets:

    fact = datasets["fact_order_item_final"]

    # --------------------------------------------------------
    # Fact → Orders
    # --------------------------------------------------------

    if "orders" in datasets:

        order_ids = (
            datasets["orders"]
            .select("order_id")
            .distinct()
        )

        orphan_orders = (
            fact
            .select("order_id")
            .distinct()
            .join(
                order_ids,
                on="order_id",
                how="left_anti"
            )
            .count()
        )

        print_check(
            "Fact → Orders",
            orphan_orders == 0,
            f"Orphan order IDs: {orphan_orders}"
        )

    # --------------------------------------------------------
    # Fact → Customers
    # --------------------------------------------------------

    if "customers" in datasets:

        customer_ids = (
            datasets["customers"]
            .select("customer_id")
            .distinct()
        )

        orphan_customers = (
            fact
            .select("customer_id")
            .distinct()
            .join(
                customer_ids,
                on="customer_id",
                how="left_anti"
            )
            .count()
        )

        print_check(
            "Fact → Customers",
            orphan_customers == 0,
            f"Orphan customer IDs: {orphan_customers}"
        )

    # --------------------------------------------------------
    # Fact → Products
    # --------------------------------------------------------

    if "products" in datasets:

        product_ids = (
            datasets["products"]
            .select("product_id")
            .distinct()
        )

        orphan_products = (
            fact
            .select("product_id")
            .distinct()
            .join(
                product_ids,
                on="product_id",
                how="left_anti"
            )
            .count()
        )

        print_check(
            "Fact → Products",
            orphan_products == 0,
            f"Orphan product IDs: {orphan_products}"
        )

    # --------------------------------------------------------
    # Fact → Sellers
    # --------------------------------------------------------

    if "sellers" in datasets:

        seller_ids = (
            datasets["sellers"]
            .select("seller_id")
            .distinct()
        )

        orphan_sellers = (
            fact
            .select("seller_id")
            .distinct()
            .join(
                seller_ids,
                on="seller_id",
                how="left_anti"
            )
            .count()
        )

        print_check(
            "Fact → Sellers",
            orphan_sellers == 0,
            f"Orphan seller IDs: {orphan_sellers}"
        )


# ============================================================
# WRITE AUDIT LOG
# ============================================================

print()
print("============================================================")
print("                 AUDIT LOGGING")
print("============================================================")

write_audit_records(
    spark,
    audit_records
)


# ============================================================
# FINAL STATUS
# ============================================================

print()
print("============================================================")
print("             DATA QUALITY FINAL STATUS")
print("============================================================")


if quality_failed:

    print("STATUS: FAILED")
    print()
    print(
        "One or more data quality checks failed."
    )
    print(
        "Pipeline execution should be stopped."
    )

    spark.stop()

    sys.exit(1)

else:

    print("STATUS: PASSED")
    print()
    print(
        "All configured data quality checks passed successfully."
    )
    print(
        "Pipeline is safe to continue."
    )

print("============================================================")

spark.stop()