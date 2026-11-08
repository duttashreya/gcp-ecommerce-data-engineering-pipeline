from pyspark.sql.functions import col


VALID_ORDER_STATUSES = {
    "delivered",
    "shipped",
    "canceled",
    "unavailable",
    "invoiced",
    "processing",
    "created",
    "approved",
}


def validate_incremental_orders_batch(df):

    results = []

    # =========================================================
    # 1. Row count
    # =========================================================

    row_count = df.count()

    if row_count > 0:
        results.append({
            "check": "batch_row_count",
            "status": "PASS",
            "details": f"Batch contains {row_count} rows."
        })
    else:
        results.append({
            "check": "batch_row_count",
            "status": "FAIL",
            "details": "Batch contains zero rows."
        })

    # =========================================================
    # 2. Required columns
    # =========================================================

    required_columns = [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_purchase_date",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if not missing_columns:
        results.append({
            "check": "required_columns",
            "status": "PASS",
            "details": "All required columns are present."
        })
    else:
        results.append({
            "check": "required_columns",
            "status": "FAIL",
            "details": (
                f"Missing columns: {missing_columns}"
            )
        })

    # =========================================================
    # 3. NULL order_id
    # =========================================================

    null_order_ids = (
        df.filter(col("order_id").isNull()).count()
    )

    if null_order_ids == 0:
        results.append({
            "check": "order_id_not_null",
            "status": "PASS",
            "details": "No NULL order IDs."
        })
    else:
        results.append({
            "check": "order_id_not_null",
            "status": "FAIL",
            "details": (
                f"Found {null_order_ids} NULL order IDs."
            )
        })

    # =========================================================
    # 4. NULL customer_id
    # =========================================================

    null_customer_ids = (
        df.filter(col("customer_id").isNull()).count()
    )

    if null_customer_ids == 0:
        results.append({
            "check": "customer_id_not_null",
            "status": "PASS",
            "details": "No NULL customer IDs."
        })
    else:
        results.append({
            "check": "customer_id_not_null",
            "status": "FAIL",
            "details": (
                f"Found {null_customer_ids} NULL customer IDs."
            )
        })

    # =========================================================
    # 5. NULL purchase timestamp
    # =========================================================

    null_purchase_timestamps = (
        df
        .filter(
            col("order_purchase_timestamp").isNull()
        )
        .count()
    )

    if null_purchase_timestamps == 0:
        results.append({
            "check": "purchase_timestamp_not_null",
            "status": "PASS",
            "details": (
                "No NULL order purchase timestamps."
            )
        })
    else:
        results.append({
            "check": "purchase_timestamp_not_null",
            "status": "FAIL",
            "details": (
                f"Found {null_purchase_timestamps} "
                "NULL purchase timestamps."
            )
        })

    # =========================================================
    # 6. NULL purchase date
    # =========================================================

    null_purchase_dates = (
        df
        .filter(
            col("order_purchase_date").isNull()
        )
        .count()
    )

    if null_purchase_dates == 0:
        results.append({
            "check": "purchase_date_not_null",
            "status": "PASS",
            "details": (
                "No NULL order purchase dates."
            )
        })
    else:
        results.append({
            "check": "purchase_date_not_null",
            "status": "FAIL",
            "details": (
                f"Found {null_purchase_dates} "
                "NULL purchase dates."
            )
        })

    # =========================================================
    # 7. Duplicate order IDs
    # =========================================================

    duplicate_order_ids = (
        df
        .groupBy("order_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    if duplicate_order_ids == 0:
        results.append({
            "check": "order_id_unique",
            "status": "PASS",
            "details": "Order IDs are unique."
        })
    else:
        results.append({
            "check": "order_id_unique",
            "status": "FAIL",
            "details": (
                f"Found {duplicate_order_ids} "
                "duplicate order IDs."
            )
        })

    # =========================================================
    # 8. Valid order status
    # =========================================================

    invalid_status_count = (
        df
        .filter(
            ~col("order_status").isin(
                list(VALID_ORDER_STATUSES)
            )
        )
        .count()
    )

    if invalid_status_count == 0:
        results.append({
            "check": "valid_order_status",
            "status": "PASS",
            "details": "All order statuses are valid."
        })
    else:
        results.append({
            "check": "valid_order_status",
            "status": "FAIL",
            "details": (
                f"Found {invalid_status_count} "
                "records with invalid order status."
            )
        })

    # =========================================================
    # Final status
    # =========================================================

    failed_checks = [
        result
        for result in results
        if result["status"] == "FAIL"
    ]

    overall_status = (
        "PASS"
        if len(failed_checks) == 0
        else "FAIL"
    )

    return results, overall_status
