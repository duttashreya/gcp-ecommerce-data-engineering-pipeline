
from pathlib import Path
import pandas as pd

# Locate the raw data folder
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_FOLDER = PROJECT_ROOT / "data" / "raw"

# All expected source files
files = [
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv"
]

print("Starting Olist data profiling...\n")

for file_name in files:

    file_path = DATA_FOLDER / file_name

    print("=" * 70)
    print(f"FILE: {file_name}")

    if not file_path.exists():
        print("ERROR: File not found!")
        continue

    # Read the CSV
    df = pd.read_csv(file_path)

    # Basic profiling
    print("Number of rows:", len(df))
    print("Number of columns:", len(df.columns))

    print("\nColumn names:")
    print(df.columns.tolist())

    print("\nData types:")
    print(df.dtypes)

    print("\nMissing values:")
    print(df.isnull().sum())

    print("\nDuplicate rows:", df.duplicated().sum())

    print("\nSample records:")
    print(df.head(5).to_string(index=False))

print("\nData profiling completed!")
