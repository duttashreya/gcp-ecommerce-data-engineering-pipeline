# GCP E-Commerce Data Engineering Pipeline

An end-to-end data engineering project built using the Olist Brazilian E-Commerce dataset.

## Project Overview

This project demonstrates a production-style e-commerce data pipeline using:

- Python
- PySpark
- Apache Airflow
- GCP-oriented data engineering architecture
- Data quality validation
- Incremental processing
- Idempotent pipeline design
- Silver and Gold data layers
- Audit logging
- Analytics-ready datasets

The source dataset is static historical data. Incremental ingestion is simulated by partitioning historical orders by `order_purchase_date`.

## Architecture

![E-Commerce Data Engineering Pipeline Architecture](docs/architecture.png)

## Airflow Orchestration

The pipeline is orchestrated using Apache Airflow with four sequential tasks:

1. Create Incremental Batch
2. Incremental Orders Silver
3. Incremental Fact Order Item Gold
4. Incremental Sales Daily Gold

![Airflow DAG Successful Run](docs/airflow_dag_success.png)

## Data Quality

The pipeline includes configuration-driven data quality validation covering:

- Row-count validation
- Required-column validation
- Primary-key null checks
- Primary-key uniqueness
- Business-rule validation
- Referential integrity
- Incremental batch validation
- Audit logging

### Latest Validation Run

| Dataset | Rows | Status |
|---|---:|---|
| orders | 99,441 | PASS |
| customers | 99,441 | PASS |
| order_items | 112,650 | PASS |
| products | 32,951 | PASS |
| sellers | 3,095 | PASS |
| payments | 103,886 | PASS |
| reviews | 99,224 | PASS |
| fact_order_item_final | 112,650 | PASS |

**Overall Status: PASSED**

## Pipeline Flow

```text
Olist CSV Dataset
       |
       v
Python Ingestion & Validation
       |
       v
Raw / Bronze Layer
       |
       v
PySpark Silver Layer
       |
       v
Gold Analytics Layer
       |
       +----------------------+
       |                      |
       v                      v
Fact / Dimension Tables   Performance Metrics
       |
       v
BI / Analytics
```

## Data Layers

### Bronze / Raw

Original source CSV files are treated as raw input.

Raw data is intentionally excluded from GitHub.

### Silver

The Silver layer standardizes schemas, timestamps, column names, and data quality.

Datasets include:

- Orders
- Customers
- Order Items
- Products
- Sellers
- Payments
- Reviews

### Gold

The Gold layer contains analytics-ready fact and dimension datasets.

Examples:

- `fact_order_item`
- `fact_order_item_final`
- `dim_customer`
- `dim_product`
- `dim_seller`
- `dim_date`
- `sales_daily`
- `sales_by_category`
- `seller_performance`
- `customer_performance`
- `product_performance`
- `delivery_performance`
- `payment_method_performance`

## Incremental Pipeline

The project simulates daily incremental ingestion using historical order dates.

```text
Batch Date
    |
    v
Create Incremental Batch
    |
    v
Incremental Orders Silver
    |
    v
Incremental Fact Order Item
    |
    v
Incremental Sales Daily
```

The incremental pipeline is designed to be **idempotent**.

If the same batch is processed again, duplicate orders are removed rather than duplicated in the consolidated Silver layer.

## Airflow DAG Configuration

The Airflow DAG is named:

`ecommerce_incremental_pipeline`

The DAG accepts a `batch_date` through the DAG run configuration.

Example:

```json
{
  "batch_date": "2018-01-03"
}
```

### DAG Task Dependencies

```text
create_incremental_batch
          |
          v
incremental_orders_silver
          |
          v
incremental_fact_order_item_gold
          |
          v
incremental_sales_daily_gold
```

The pipeline uses sequential task dependencies so that each stage runs only after the previous stage completes successfully.

## Data Quality Framework

The project includes configuration-driven and incremental data quality validation.

### Quality Checks

- Row count validation
- Required column validation
- Primary key null checks
- Primary key uniqueness
- Business rule validation
- Referential integrity
- Incremental batch validation
- Audit logging

### Business Rules

- Order IDs must not be NULL
- Order IDs must be unique
- Payment values must be non-negative
- Review scores must be between 1 and 5
- Order item revenue must be non-negative
- Fact records must reference valid customers
- Fact records must reference valid products
- Fact records must reference valid sellers

## Audit Logging

Pipeline execution metadata is written to audit datasets.

Audit information includes:

- Run ID
- Pipeline name
- Run timestamp
- Dataset
- Layer
- Row counts
- Pipeline status
- Error information
- Incremental batch metrics

The audit framework supports both standard pipeline runs and incremental processing runs.

## Example Incremental Pipeline Result

### Batch Date: `2018-01-03`

| Metric | Result |
|---|---:|
| Incoming Orders | 225 |
| Incremental Fact Rows | 247 |
| Distinct Orders | 222 |
| Total Items | 247 |
| Total Revenue | 36,746.08 |
| Average Order Value | 165.52 |
| Pipeline Status | PASSED |

The example demonstrates successful incremental processing and idempotent reruns.

## Project Structure

```text
gcp-ecommerce-data-engineering-pipeline/
│
├── dags/
│   └── ecommerce_incremental_pipeline.py
│
├── ingestion/
│   ├── create_incremental_batch.py
│   ├── incremental_orders_silver.py
│   ├── profile_data.py
│   ├── run_incremental_pipeline.py
│   └── validate_data.py
│
├── spark_jobs/
│   ├── Silver transformation jobs
│   ├── Gold dimension jobs
│   ├── Gold fact jobs
│   ├── Performance analytics jobs
│   └── Incremental Gold jobs
│
├── tests/
│   ├── audit_logger.py
│   ├── data_quality_config.py
│   ├── data_quality_framework.py
│   ├── incremental_data_quality.py
│   └── verify_silver.py
│
├── docs/
│   ├── architecture.png
│   ├── airflow_dag_success.png
│   └── data_quality_rules.md
│
├── requirements.txt
├── README.md
└── .gitignore
```

## Data Engineering Concepts Demonstrated

- ETL / ELT pipeline design
- Batch processing
- Incremental data processing
- Idempotency
- PySpark transformations
- Data quality engineering
- Referential integrity
- Dimensional modeling
- Fact and dimension tables
- Audit logging
- Workflow orchestration
- Failure handling
- Pipeline validation
- Analytics-ready data modeling

## Technologies

| Technology | Purpose |
|---|---|
| Python | Ingestion, validation, and pipeline execution |
| PySpark | Data transformation and processing |
| Apache Airflow | Workflow orchestration |
| GCP | Target cloud architecture |
| BigQuery | Target analytical warehouse |
| Git / GitHub | Version control |

## Dataset

The project uses the **Olist Brazilian E-Commerce Public Dataset**.

The dataset contains e-commerce information including:

- Orders
- Customers
- Order Items
- Products
- Sellers
- Payments
- Reviews

The original raw dataset is not included in this repository.

## Future Improvements

- Deploy raw data ingestion to Google Cloud Storage
- Run Spark transformations on Google Cloud Dataproc
- Load Gold datasets into BigQuery
- Deploy orchestration using Cloud Composer
- Add CI/CD using GitHub Actions
- Build Power BI or Tableau dashboards
- Add pipeline monitoring and alerting
- Add production-style configuration management
- Add cloud-based data quality monitoring

## Author

Data Engineering portfolio project demonstrating practical experience with Python, SQL, PySpark, cloud data engineering, data quality, incremental processing, workflow orchestration, and analytics-ready data modeling.