from pyspark import pipelines as dp
from pyspark.sql.functions import expr

# ---------------------------------------------------------------------------
# bronze_orders (CDC) — taught in depth in the lecture
# ---------------------------------------------------------------------------

ORDERS_RULES = {
    "valid_order_id": "after.order_id IS NOT NULL",
    "valid_customer_id": "after.customer_id IS NOT NULL",
    "valid_total_amount": "(after.total_amount IS NULL OR after.total_amount >= 0)"

}

def _quarantine_rule(rules:dict) -> str:
    return "NOT({0})".format(" AND ".join(rules.values()))

@dp.table(private=True, name="bronze_orders_quality_check", partition_cols=["is_quarantined"])
@dp.expect_all(ORDERS_RULES)
def bronze_orders_quality_check():
    return spark.readStream.table("bronze_orders")\
        .withColumn("is_quarantined", expr(_quarantine_rule(ORDERS_RULES)))

@dp.table(name="bronze_orders_valid", comment="Orders that passed every structural quality check. Published — read by Silver from Module 3 onward.")

def bronze_orders_valid():
    return spark.readStream.table("bronze_orders_quality_check").filter("is_quarantined = false")

@dp.table(name="bronze_orders_quarantined", comment="Orders that failed at least one structural quality check. Published — monitored in L19.")

def bronze_orders_quarantined():
    return spark.readStream.table("bronze_orders_quality_check").filter("is_quarantined = true")

# ---------------------------------------------------------------------------
# bronze_products (file-based) — taught in depth in the lecture
# ---------------------------------------------------------------------------

PRODUCT_RULES = {
    "valid_product_id": "product_id IS NOT NULL",
    "valid_sku": "sku IS NOT NULL",
    "valid_retail_price": "retail_price IS NULL OR retail_price > 0",
}

@dp.table(private=True, name = "bronze_products_quality_check", partition_cols = ["is_quarantined"])
@dp.expect_all(PRODUCT_RULES)
def bronze_products_quality_check():
    return spark.readStream.table("bronze_products")\
        .withColumn("is_quarantined", expr(_quarantine_rule(PRODUCT_RULES)))

@dp.table(name = "bronze_products_valid")
def bronze_products_valid():
    return spark.readStream.table("bronze_products_quality_check").filter("is_quarantined = false")


@dp.table(name = "bronze_products_quarantined")
def bronze_products_quarantined():
    return spark.readStream.table("bronze_products_quality_check").filter("is_quarantined = true")

# ---------------------------------------------------------------------------
# bronze_order_items (CDC) — same shape, walked briefly in the lecture
# ---------------------------------------------------------------------------

ORDER_ITEMS_RULES = {
    "valid_order_item_id": "after.order_item_id IS NOT NULL",
    "valid_order_ref": "after.order_id IS NOT NULL",
    "valid_product_ref": "after.product_id IS NOT NULL",
    "valid_quantity": "after.quantity IS NULL OR after.quantity > 0",
}


@dp.table(private=True, partition_cols=["is_quarantined"])
@dp.expect_all(ORDER_ITEMS_RULES)
def bronze_order_items_quality_check():
    return spark.readStream.table("bronze_order_items").withColumn(
        "is_quarantined", expr(_quarantine_rule(ORDER_ITEMS_RULES))
    )


@dp.table(comment="Order items that passed every structural quality check. Published — read by Silver from Module 3 onward.")
def bronze_order_items_valid():
    return spark.readStream.table("bronze_order_items_quality_check").filter("is_quarantined = false")


@dp.table(comment="Order items that failed at least one structural quality check. Published — monitored in L19.")
def bronze_order_items_quarantined():
    return spark.readStream.table("bronze_order_items_quality_check").filter("is_quarantined = true")


# ---------------------------------------------------------------------------
# bronze_customers (CDC) — same shape, walked briefly in the lecture
# ---------------------------------------------------------------------------

CUSTOMERS_RULES = {
    "valid_customer_id": "after.customer_id IS NOT NULL",
    "valid_email": "after.email IS NOT NULL",
    "valid_loyalty_tier": "after.loyalty_tier IS NULL OR after.loyalty_tier IN ('bronze', 'silver', 'gold', 'platinum')",
}


@dp.table(private=True, partition_cols=["is_quarantined"])
@dp.expect_all(CUSTOMERS_RULES)
def bronze_customers_quality_check():
    return spark.readStream.table("bronze_customers").withColumn(
        "is_quarantined", expr(_quarantine_rule(CUSTOMERS_RULES))
    )


@dp.table(comment="Customers that passed every structural quality check. Published — read by Silver from Module 3 onward.")
def bronze_customers_valid():
    return spark.readStream.table("bronze_customers_quality_check").filter("is_quarantined = false")


@dp.table(comment="Customers that failed at least one structural quality check. Published — monitored in L19.")
def bronze_customers_quarantined():
    return spark.readStream.table("bronze_customers_quality_check").filter("is_quarantined = true")


# ---------------------------------------------------------------------------
# bronze_categories (file-based) — same shape, walked briefly in the lecture
# ---------------------------------------------------------------------------

CATEGORIES_RULES = {
    "valid_category_id": "category_id IS NOT NULL",
    "valid_category_name": "category_name IS NOT NULL",
}


@dp.table(private=True, partition_cols=["is_quarantined"])
@dp.expect_all(CATEGORIES_RULES)
def bronze_categories_quality_check():
    return spark.readStream.table("bronze_categories").withColumn(
        "is_quarantined", expr(_quarantine_rule(CATEGORIES_RULES))
    )


@dp.table(comment="Categories that passed every structural quality check. Published — read by Silver from Module 3 onward.")
def bronze_categories_valid():
    return spark.readStream.table("bronze_categories_quality_check").filter("is_quarantined = false")


@dp.table(comment="Categories that failed at least one structural quality check. Published — monitored in L19.")
def bronze_categories_quarantined():
    return spark.readStream.table("bronze_categories_quality_check").filter("is_quarantined = true")


# ---------------------------------------------------------------------------
# bronze_clickstream (file-based) — same shape, walked briefly in the lecture
# ---------------------------------------------------------------------------

CLICKSTREAM_RULES = {
    "valid_event_id": "event_id IS NOT NULL",
    "valid_event_type": "event_type IS NOT NULL",
    "valid_event_timestamp": "event_timestamp IS NOT NULL",
}


@dp.table(private=True, partition_cols=["is_quarantined"])
@dp.expect_all(CLICKSTREAM_RULES)
def bronze_clickstream_quality_check():
    return spark.readStream.table("bronze_clickstream").withColumn(
        "is_quarantined", expr(_quarantine_rule(CLICKSTREAM_RULES))
    )


@dp.table(comment="Clickstream events that passed every structural quality check. Published — read by Silver from Module 3 onward.")
def bronze_clickstream_valid():
    return spark.readStream.table("bronze_clickstream_quality_check").filter("is_quarantined = false")


@dp.table(comment="Clickstream events that failed at least one structural quality check. Published — monitored in L19.")
def bronze_clickstream_quarantined():
    return spark.readStream.table("bronze_clickstream_quality_check").filter("is_quarantined = true")


# ---------------------------------------------------------------------------
# bronze_inventory (file-based) — same shape, walked briefly in the lecture
# ---------------------------------------------------------------------------

INVENTORY_RULES = {
    "valid_snapshot_id": "snapshot_id IS NOT NULL",
    "valid_product_ref": "product_id IS NOT NULL",
    "valid_quantity_on_hand": "quantity_on_hand IS NULL OR quantity_on_hand >= 0",
}


@dp.table(private=True, partition_cols=["is_quarantined"])
@dp.expect_all(INVENTORY_RULES)
def bronze_inventory_quality_check():
    return spark.readStream.table("bronze_inventory").withColumn(
        "is_quarantined", expr(_quarantine_rule(INVENTORY_RULES))
    )


@dp.table(comment="Inventory snapshots that passed every structural quality check. Published — read by Silver from Module 3 onward.")
def bronze_inventory_valid():
    return spark.readStream.table("bronze_inventory_quality_check").filter("is_quarantined = false")


@dp.table(comment="Inventory snapshots that failed at least one structural quality check. Published — monitored in L19.")
def bronze_inventory_quarantined():
    return spark.readStream.table("bronze_inventory_quality_check").filter("is_quarantined = true")
