from pathlib import Path
import pandas as pd

# ---------------------------------------------------------
# PROJECT PATH
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_FOLDER = PROJECT_ROOT / "data" / "raw"


# ---------------------------------------------------------
# HELPER FUNCTION
# ---------------------------------------------------------

def load_csv(filename):
    """Load a CSV file from the raw data folder."""
    file_path = DATA_FOLDER / filename

    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    return pd.read_csv(file_path)


# ---------------------------------------------------------
# LOAD TABLES
# ---------------------------------------------------------

print("\nLoading datasets...\n")

orders = load_csv("olist_orders_dataset.csv")
customers = load_csv("olist_customers_dataset.csv")
order_items = load_csv("olist_order_items_dataset.csv")
payments = load_csv("olist_order_payments_dataset.csv")
reviews = load_csv("olist_order_reviews_dataset.csv")
products = load_csv("olist_products_dataset.csv")
sellers = load_csv("olist_sellers_dataset.csv")

print("All required datasets loaded successfully.")


# ---------------------------------------------------------
# 1. ROW COUNTS
# ---------------------------------------------------------

print("\n" + "=" * 60)
print("ROW COUNTS")
print("=" * 60)

datasets = {
    "orders": orders,
    "customers": customers,
    "order_items": order_items,
    "payments": payments,
    "reviews": reviews,
    "products": products,
    "sellers": sellers,
}

for name, df in datasets.items():
    print(f"{name:15} : {len(df):,} rows")


# ---------------------------------------------------------
# 2. PRIMARY KEY DUPLICATE CHECK
# ---------------------------------------------------------

print("\n" + "=" * 60)
print("DUPLICATE KEY CHECK")
print("=" * 60)

key_checks = {
    "orders": ("order_id", orders),
    "customers": ("customer_id", customers),
    "order_items": ("order_id + order_item_id", order_items),
    "payments": ("order_id + payment_sequential", payments),
    "reviews": ("review_id", reviews),
    "products": ("product_id", products),
    "sellers": ("seller_id", sellers),
}

for table_name, (key_name, df) in key_checks.items():

    if table_name == "order_items":
        duplicates = df.duplicated(
            subset=["order_id", "order_item_id"]
        ).sum()

    elif table_name == "payments":
        duplicates = df.duplicated(
            subset=["order_id", "payment_sequential"]
        ).sum()

    else:
        duplicates = df[key_name].duplicated().sum()

    print(f"{table_name:15} : {duplicates:,} duplicate keys")


# ---------------------------------------------------------
# 3. NULL VALUE CHECK
# ---------------------------------------------------------

print("\n" + "=" * 60)
print("NULL VALUE CHECK")
print("=" * 60)

for name, df in datasets.items():

    total_nulls = df.isnull().sum().sum()

    print(f"{name:15} : {total_nulls:,} total NULL values")


# ---------------------------------------------------------
# 4. ORDER STATUS
# ---------------------------------------------------------

print("\n" + "=" * 60)
print("ORDER STATUS")
print("=" * 60)

print(orders["order_status"].value_counts())


# ---------------------------------------------------------
# 5. ORDER DATE RANGE
# ---------------------------------------------------------

print("\n" + "=" * 60)
print("ORDER DATE RANGE")
print("=" * 60)

orders["order_purchase_timestamp"] = pd.to_datetime(
    orders["order_purchase_timestamp"]
)

print(
    "First order:",
    orders["order_purchase_timestamp"].min()
)

print(
    "Latest order:",
    orders["order_purchase_timestamp"].max()
)


# ---------------------------------------------------------
# 6. PAYMENT TYPES
# ---------------------------------------------------------

print("\n" + "=" * 60)
print("PAYMENT TYPES")
print("=" * 60)

print(payments["payment_type"].value_counts())


# ---------------------------------------------------------
# 7. PRODUCT CATEGORIES
# ---------------------------------------------------------

print("\n" + "=" * 60)
print("TOP PRODUCT CATEGORIES")
print("=" * 60)

print(
    products["product_category_name"]
    .value_counts()
    .head(15)
)


# ---------------------------------------------------------
# 8. REFERENTIAL INTEGRITY
# ---------------------------------------------------------

print("\n" + "=" * 60)
print("REFERENTIAL INTEGRITY CHECKS")
print("=" * 60)


# Orders -> Customers

orphan_orders = ~orders["customer_id"].isin(
    customers["customer_id"]
)

print(
    "Orders without customer:",
    orphan_orders.sum()
)


# Order Items -> Orders

orphan_order_items = ~order_items["order_id"].isin(
    orders["order_id"]
)

print(
    "Order items without order:",
    orphan_order_items.sum()
)


# Order Items -> Products

orphan_products = ~order_items["product_id"].isin(
    products["product_id"]
)

print(
    "Order items without product:",
    orphan_products.sum()
)


# Order Items -> Sellers

orphan_sellers = ~order_items["seller_id"].isin(
    sellers["seller_id"]
)

print(
    "Order items without seller:",
    orphan_sellers.sum()
)


print("\n" + "=" * 60)
print("VALIDATION COMPLETE")
print("=" * 60)

# ---------------------------------------------------------
# REVIEW DUPLICATE INVESTIGATION
# ---------------------------------------------------------

print("\n" + "=" * 60)
print("REVIEW DUPLICATE INVESTIGATION")
print("=" * 60)

review_duplicates = reviews[
    reviews.duplicated(
        subset=["review_id"],
        keep=False
    )
].sort_values("review_id")

print("\nNumber of rows involved in duplicate review IDs:")
print(len(review_duplicates))

print("\nSample duplicate review IDs:")
print(
    review_duplicates[
        [
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp"
        ]
    ].head(20).to_string(index=False)
)

print("\nDuplicate review_id frequency:")
print(
    reviews["review_id"]
    .value_counts()
    .loc[lambda x: x > 1]
    .head(20)
)
# ---------------------------------------------------------
# NULL VALUES BY COLUMN
# ---------------------------------------------------------

print("\n" + "=" * 60)
print("NULL VALUES BY COLUMN")
print("=" * 60)

for name, df in datasets.items():

    print(f"\n--- {name.upper()} ---")

    null_counts = df.isnull().sum()

    null_counts = null_counts[
        null_counts > 0
    ].sort_values(ascending=False)

    if len(null_counts) == 0:
        print("No NULL values.")

    else:
        print(null_counts)

    # ---------------------------------------------------------
# ORDER NULL ANALYSIS
# ---------------------------------------------------------

print("\n" + "=" * 60)
print("ORDER NULL ANALYSIS")
print("=" * 60)

order_date_columns = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

for column in order_date_columns:

    print(f"\n{column}")

    result = (
        orders
        .groupby("order_status")[column]
        .apply(lambda x: x.isnull().sum())
    )

    print(result)

# ---------------------------------------------------------
# PRODUCT NULL ANALYSIS
# ---------------------------------------------------------

print("\n" + "=" * 60)
print("PRODUCT NULL ANALYSIS")
print("=" * 60)

product_nulls = products.isnull().sum()

print(
    product_nulls[
        product_nulls > 0
    ].sort_values(ascending=False)
)