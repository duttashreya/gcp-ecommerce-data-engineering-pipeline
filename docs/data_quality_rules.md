# Data Quality Rules

## Overview

The e-commerce pipeline uses a configuration-driven data quality framework to validate data across the Silver and Gold layers.

The framework validates:

- Row counts
- Required columns
- Primary key nullability
- Primary key uniqueness
- Business rules
- Referential integrity
- Incremental batch quality
- Audit logging

A pipeline run is marked as **PASSED** only when all mandatory data quality checks pass.

---

## 1. Row Count Validation

Each required dataset must contain records after processing.

| Dataset | Minimum Expected Rows |
|---|---:|
| orders | 1 |
| customers | 1 |
| order_items | 1 |
| products | 1 |
| sellers | 1 |
| payments | 1 |
| reviews | 1 |
| fact_order_item_final | 1 |

A dataset with zero records is treated as a data quality failure.

---

## 2. Required Column Validation

Required columns must exist before downstream processing.

### Orders

Required columns:

- `order_id`
- `customer_id`
- `order_status`
- `order_purchase_timestamp`
- `order_purchase_date`

### Customers

Required columns:

- `customer_id`
- `customer_unique_id`
- `customer_zip_code_prefix`
- `customer_city`
- `customer_state`

### Order Items

Required columns:

- `order_id`
- `order_item_id`
- `product_id`
- `seller_id`
- `shipping_limit_date`
- `price`
- `freight_value`

### Products

Required columns:

- `product_id`
- `product_category`

### Sellers

Required columns:

- `seller_id`
- `seller_zip_code_prefix`
- `seller_city`
- `seller_state`

### Payments

Required columns:

- `order_id`
- `payment_sequential`
- `payment_type`
- `payment_installments`
- `payment_value`

### Reviews

Required columns:

- `review_id`
- `order_id`
- `review_score`

### Final Fact

Required columns:

- `order_id`
- `order_item_id`
- `customer_id`
- `product_id`
- `seller_id`
- `order_purchase_date`
- `order_status`
- `price`
- `freight_value`
- `total_item_value`

---

## 3. Primary Key Validation

Primary keys must satisfy two conditions:

1. Primary key values must not be NULL.
2. Primary key combinations must be unique.

### Primary Keys

| Dataset | Primary Key |
|---|---|
| orders | `order_id` |
| customers | `customer_id` |
| order_items | `order_id`, `order_item_id` |
| products | `product_id` |
| sellers | `seller_id` |
| payments | `order_id`, `payment_sequential` |
| reviews | `review_id`, `order_id` |
| fact_order_item_final | `order_id`, `order_item_id` |

### Composite Keys

Some datasets require composite keys because a single column does not uniquely identify a record.

For example:

```text
order_items
Primary Key = (order_id, order_item_id)
```

and:

```text
payments
Primary Key = (order_id, payment_sequential)
```

The reviews dataset uses:

```text
(review_id, order_id)
```

because `review_id` alone is not sufficient to guarantee uniqueness in the source data.

---

## 4. NULL Validation

Critical identifier and timestamp fields must not contain NULL values.

### Orders

The following fields must not be NULL:

- `order_id`
- `customer_id`
- `order_purchase_timestamp`
- `order_purchase_date`

### Incremental Orders

The following fields must not be NULL:

- `order_id`
- `customer_id`
- `order_purchase_timestamp`
- `order_purchase_date`

### Final Fact

The following fields must not be NULL:

- `order_id`
- `order_item_id`
- `customer_id`
- `product_id`
- `seller_id`

Nullable descriptive fields are allowed where NULL values are valid in the source dataset.

---

## 5. Business Rule Validation

Business rules ensure that numeric and categorical values fall within valid ranges.

### Order Items

The following rules are applied:

```text
price >= 0
freight_value >= 0
```

### Payments

Payment amounts must be non-negative:

```text
payment_value >= 0
```

### Reviews

Review scores must be between 1 and 5:

```text
review_score BETWEEN 1 AND 5
```

### Final Fact

Item-level revenue must be non-negative:

```text
total_item_value >= 0
```

The item-level revenue metric is calculated as:

```text
total_item_value = price + freight_value
```

This metric is used for item-level sales aggregation.

---

## 6. Order Status Validation

Orders must contain a valid order status.

Allowed values are:

```text
delivered
shipped
canceled
unavailable
invoiced
processing
created
approved
```

Any unexpected order status is treated as a data quality failure.

---

## 7. Referential Integrity

The final fact table must reference valid records from the corresponding dimension and source datasets.

The following relationships are validated:

```text
fact_order_item_final.order_id
        -> orders.order_id
```

```text
fact_order_item_final.customer_id
        -> customers.customer_id
```

```text
fact_order_item_final.product_id
        -> products.product_id
```

```text
fact_order_item_final.seller_id
        -> sellers.seller_id
```

Invalid foreign-key references are treated as data quality failures.

---

## 8. Incremental Data Quality

Incremental batches are validated before being merged into the consolidated Silver layer.

The incremental framework validates:

- Batch row count
- Required columns
- NULL order IDs
- NULL customer IDs
- NULL purchase timestamps
- NULL purchase dates
- Duplicate order IDs
- Valid order statuses

### Incremental Validation Rules

```text
batch_row_count > 0
```

```text
order_id IS NOT NULL
```

```text
customer_id IS NOT NULL
```

```text
order_purchase_timestamp IS NOT NULL
```

```text
order_purchase_date IS NOT NULL
```

```text
order_id must be unique within the batch
```

---

## 9. Incremental Batch Validation Flow

The incremental pipeline validates incoming data before processing it further.

```text
Incoming Batch
      |
      v
Required Column Check
      |
      v
NULL Validation
      |
      v
Duplicate Validation
      |
      v
Business Rule Validation
      |
      v
Incremental DQ Status
      |
      +----------------+
      |                |
     PASS             FAIL
      |                |
      v                v
Silver Merge       Pipeline Stops
```

A failed incremental data quality check prevents the batch from being merged into the downstream processing layer.

---

## 10. Idempotency Validation

The incremental pipeline is designed to be **idempotent**.

The same batch can be processed multiple times without creating duplicate records in the consolidated Silver layer.

### Idempotent Processing

```text
Incoming Batch
      |
      v
Existing Silver Data
      |
      v
Union Existing + Incoming
      |
      v
Deduplicate by order_id
      |
      v
Keep Latest Record
      |
      v
Consolidated Silver
```

For each `order_id`, the latest `order_purchase_timestamp` is retained.

This prevents duplicate records from accumulating when an already processed batch is rerun.

---

## 11. Incremental Audit Metrics

Each incremental pipeline run records processing metrics.

The audit log captures:

| Metric | Description |
|---|---|
| `run_id` | Unique pipeline execution ID |
| `pipeline_name` | Pipeline that executed |
| `batch_date` | Incremental batch date |
| `dataset_name` | Dataset being processed |
| `input_row_count` | Incoming batch rows |
| `existing_row_count` | Existing Silver rows |
| `combined_row_count` | Rows before deduplication |
| `duplicates_removed` | Duplicate rows removed |
| `final_row_count` | Final Silver row count |
| `status` | PASS or FAIL |
| `error_message` | Error details when applicable |

---

## 12. Audit Logging

Standard pipeline execution metadata is also recorded.

Audit information includes:

- Run ID
- Pipeline name
- Run timestamp
- Dataset name
- Layer
- Row count
- Pipeline status
- Error information

Audit records provide traceability for pipeline executions and support troubleshooting.

---

## 13. Pipeline Failure Handling

The pipeline follows a fail-fast approach for critical data quality failures.

```text
Data Quality Check
        |
        v
      PASS
        |
        v
Continue Pipeline
```

If a critical validation fails:

```text
Data Quality Check
        |
        v
      FAIL
        |
        v
Pipeline Stops
        |
        v
Failure Recorded
        |
        v
Audit Log
```

This prevents invalid data from being propagated to downstream Gold datasets.

---

## 14. Data Quality Framework Architecture

The framework is configuration-driven.

Dataset-specific validation rules are defined separately from the validation engine.

```text
data_quality_config.py
          |
          v
data_quality_framework.py
          |
          v
Dataset Validation
          |
          +----------------------+
          |                      |
          v                      v
Structural Checks          Business Checks
          |                      |
          +----------+-----------+
                     |
                     v
             Referential Integrity
                     |
                     v
                Final Status
                     |
              +------+------+
              |             |
             PASS          FAIL
              |             |
              v             v
        Pipeline         Pipeline
        Continues         Stops
```

This approach makes it easier to add new datasets and validation rules without rewriting the complete validation framework.

---

## 15. Latest Full Pipeline Validation

The latest full data quality validation completed successfully.

| Dataset | Row Count | Status |
|---|---:|---|
| orders | 99,441 | PASS |
| customers | 99,441 | PASS |
| order_items | 112,650 | PASS |
| products | 32,951 | PASS |
| sellers | 3,095 | PASS |
| payments | 103,886 | PASS |
| reviews | 99,224 | PASS |
| fact_order_item_final | 112,650 | PASS |

### Business Quality Checks

```text
All business quality checks: PASS
```

### Referential Integrity

```text
Fact -> Orders: PASS
Fact -> Customers: PASS
Fact -> Products: PASS
Fact -> Sellers: PASS
```

### Audit Logging

```text
Audit records written successfully
```

### Final Status

```text
DATA QUALITY FINAL STATUS: PASSED
```

---

## 16. Example Incremental Validation

Example batch:

```text
Batch Date: 2018-01-03
```

Incremental validation results:

| Check | Result |
|---|---|
| Batch row count | PASS |
| Required columns | PASS |
| Order ID not NULL | PASS |
| Customer ID not NULL | PASS |
| Purchase timestamp not NULL | PASS |
| Purchase date not NULL | PASS |
| Order ID uniqueness | PASS |
| Valid order status | PASS |

### Incremental Processing Result

```text
Incoming orders: 225
Existing Silver rows: 278
Combined rows: 503
Duplicates removed: 0
Final Silver rows: 503
```

### Incremental Gold Result

```text
Incremental orders: 225
Incremental fact rows: 247
Distinct orders: 222
Total items: 247
Total revenue: 36,746.08
Average order value: 165.52
```

```text
Incremental Pipeline Status: PASSED
```

---

## 17. Data Quality Principles

The project follows these data engineering principles:

### Validate Before Transform

Incoming data is validated before downstream transformations.

### Fail Fast

Critical data quality failures stop pipeline execution.

### Maintain Referential Integrity

Fact records must reference valid dimension records.

### Enforce Uniqueness

Primary keys and business keys are validated for duplicates.

### Preserve Auditability

Every pipeline execution produces audit information.

### Support Idempotency

Repeated processing of the same batch does not create duplicate records.

### Separate Configuration From Logic

Validation rules are maintained separately from the core validation engine.

---

## Summary

The data quality framework provides automated validation across the e-commerce data pipeline.

It ensures that:

- Source data is structurally valid
- Required fields are present
- Primary keys are valid and unique
- Business rules are satisfied
- Referential integrity is maintained
- Incremental batches are validated
- Pipeline failures are captured
- Processing is auditable
- Incremental processing remains idempotent

**Final Data Quality Status: PASSED**