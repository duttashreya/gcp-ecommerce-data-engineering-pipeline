from pathlib import Path

from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SILVER_PATH = PROJECT_ROOT / "data" / "processed" / "silver"


EXPECTED_COUNTS = {
    "orders": 99441,
    "customers": 99441,
    "order_items": 112650,
    "payments": 103886,
    "products": 32951,
    "sellers": 3095,
    "reviews": 99224,
}


spark = (
    SparkSession.builder
    .appName("VerifySilverLayer")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("\n========== SILVER LAYER VERIFICATION ==========\n")

all_passed = True


for dataset, expected_count in EXPECTED_COUNTS.items():

    path = SILVER_PATH / dataset

    print(f"Checking: {dataset}")

    df = spark.read.parquet(str(path))

    actual_count = df.count()

    print(f"  Expected rows : {expected_count}")
    print(f"  Actual rows   : {actual_count}")

    if actual_count == expected_count:
        print("  Row count     : PASS")
    else:
        print("  Row count     : FAIL")
        all_passed = False

    print("  Columns       :", ", ".join(df.columns))
    print()


print("==============================================")

if all_passed:
    print("ALL SILVER DATASETS PASSED.")
else:
    print("SOME SILVER DATASETS FAILED.")

print("==============================================\n")

spark.stop()