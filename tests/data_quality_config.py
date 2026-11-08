# ============================================================
# DATA QUALITY CONFIGURATION
# ============================================================

DATA_QUALITY_CONFIG = {

    "orders": {
        "path": "data/processed/silver/orders",
        "primary_key": ["order_id"],
        "required_columns": [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_purchase_date",
        ],
        "min_rows": 1,
    },

    "customers": {
        "path": "data/processed/silver/customers",
        "primary_key": ["customer_id"],
        "required_columns": [
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ],
        "min_rows": 1,
    },

    "order_items": {
        "path": "data/processed/silver/order_items",
        "primary_key": [
            "order_id",
            "order_item_id",
        ],
        "required_columns": [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
        ],
        "min_rows": 1,
    },

    "products": {
        "path": "data/processed/silver/products",
        "primary_key": ["product_id"],
        "required_columns": [
            "product_id",
            "product_category_name",
            "product_name_length",
            "product_description_length",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ],
        "min_rows": 1,
    },

    "sellers": {
        "path": "data/processed/silver/sellers",
        "primary_key": ["seller_id"],
        "required_columns": [
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
        ],
        "min_rows": 1,
    },

    "payments": {
        "path": "data/processed/silver/payments",
        "primary_key": [
            "order_id",
            "payment_sequential",
        ],
        "required_columns": [
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
        ],
        "min_rows": 1,
    },

    "reviews": {
        "path": "data/processed/silver/reviews",

        # review_id alone is NOT unique in this dataset.
        # Therefore we use the review grain defined during
        # profiling/validation.
        "primary_key": [
            "review_id",
            "order_id",
        ],

        "required_columns": [
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp",
        ],

        "min_rows": 1,
    },

    "fact_order_item_final": {
        "path": "data/processed/gold/fact_order_item_final",
        "primary_key": [
            "order_id",
            "order_item_id",
        ],
        "required_columns": [
            "order_id",
            "order_item_id",
            "customer_id",
            "product_id",
            "seller_id",
            "order_purchase_date",
            "order_status",
            "price",
            "freight_value",
            "total_item_value",
        ],
        "min_rows": 1,
    },
}
