# Data Quality Rules

## 1. Orders

### Primary key
`order_id`

### Rules
- `order_id` must not be NULL.
- `order_id` must be unique.
- `order_purchase_timestamp` must not be NULL.
- Date columns should be converted to timestamp/date types.
- Delivery timestamps may be NULL for orders that have not been delivered.
- NULL delivery dates should not be replaced with artificial values.

---

## 2. Customers

### Primary key
`customer_id`

### Rules
- `customer_id` must not be NULL.
- `customer_id` must be unique.
- Customer location fields should be standardized.

---

## 3. Order Items

### Composite key
`order_id + order_item_id`

### Rules
- Composite key must be unique.
- `order_id` must exist in orders.
- `product_id` must exist in products.
- `seller_id` must exist in sellers.
- Price must be greater than or equal to zero.
- Freight value must be greater than or equal to zero.

---

## 4. Payments

### Composite key
`order_id + payment_sequential`

### Rules
- Composite key must be unique.
- `order_id` must exist in orders.
- Payment value must be greater than or equal to zero.
- Payment type should be standardized.

---

## 5. Reviews

### Source observation
`review_id` contains duplicate values.

### Rules
- Do not blindly deduplicate using `review_id`.
- Preserve review records.
- NULL review title is allowed.
- NULL review message is allowed.
- Review score must be between 1 and 5.

---

## 6. Products

### Primary key
`product_id`

### Rules
- `product_id` must be unique.
- Product category may be NULL.
- Product physical dimensions may be NULL.
- Do not replace missing physical measurements with zero.
- Standardize category names.

---

## 7. Sellers

### Primary key
`seller_id`

### Rules
- `seller_id` must not be NULL.
- `seller_id` must be unique.
- Seller location fields should be standardized.

---

## General Rules

- Raw Bronze data must remain unchanged.
- Silver data should contain standardized column names.
- Source timestamps should be converted to appropriate timestamp types.
- Invalid records should be quarantined rather than silently deleted.
- Every transformation should be logged.
- Data-quality checks should run before loading Gold tables.