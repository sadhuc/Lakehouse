from pyspark import pipelines as dp
from pyspark.sql.functions import col,expr
from utilities.helpers import bronze_table

# ---------------------------------------------------------------------------
# orders — SCD Type 2, report-only business rules
# ---------------------------------------------------------------------------

@dp.view
def orders_change_feed():
    return (
        spark.readStream.table(bronze_table("bronze_orders_valid"))
            .filter(col("after").isNotNull())
            .select("after.*", "op", "ts_ms")
            .withColumn("order_date", col("order_date").cast("timestamp"))
            .withColumn("updated_at", col("updated_at").cast("timestamp"))
    )

ORDERS_RULES = {
    "valid_order_status" : "order_status IN ('pending','confirmed','shipped','delivered','cancelled','returned')",
    "discount_consistency" : "(discount_code IS NULL AND discount_amount IS NULL)"
    "OR (discount_code IS NOT NULL AND discount_amount IS NOT NULL AND discount_amount > 0)"
}

dp.create_streaming_table(
    name = "silver_orders",
    comment = "Orders, SCD Type 2 full history of status change over time",
    expect_all=ORDERS_RULES
)

dp.create_auto_cdc_flow(
    target="silver_orders",
    source="orders_change_feed",
    keys=["order_id"],
    sequence_by="ts_ms",
    stored_as_scd_type=2,
    ignore_null_updates=True,
    except_column_list= ["op"]
)

# ---------------------------------------------------------------------------
# customers — SCD Type 2, report-only business rules
# ---------------------------------------------------------------------------

CUSTOMERS_RULES = {
    "valid_loyalty_tier": "loyalty_tier IN ('bronze','silver','gold','platinum')",
    "registration_not_future": "registration_date <= current_timestamp()",
    "customer_min_age": "months_between(current_date(), date_of_birth) >= 18 * 12",
}

@dp.view
def customers_change_feed():
    return(
        spark.readStream.table(bronze_table("bronze_customers_valid"))
            .filter(col("after").isNotNull())
            .select("after.*","op","ts_ms")
            .withColumn("date_of_birth", col("date_of_birth").cast("date"))
            .withColumn("registration_date", col("registration_date").cast("timestamp"))
            .withColumn("updated_at", col("updated_at").cast("timestamp"))
    )

dp.create_streaming_table(
    name = "silver_customers",
    comment = "Customers, SCD Type 2 — full history of profile and loyalty-tier changes.",
    expect_all = CUSTOMERS_RULES
)

dp.create_auto_cdc_flow(
    target = "silver_customers",
    source = "customers_change_feed",
    keys = ["customer_id"],
    sequence_by = "ts_ms",
    stored_as_scd_type = 2,
    ignore_null_updates=True,
    except_column_list= ["op"]
)

# ---------------------------------------------------------------------------
# order_items — SCD Type 1, MIXED handling: report-only + one quarantined rule
# ---------------------------------------------------------------------------

ORDER_ITEMS_RULES = {
    "return_has_reason": "NOT (return_requested = true AND return_reason IS NULL)",
    "line_total_matches": "ABS(line_total - (quantity * unit_price)) < 0.01"
}
ORDER_ITEMS_QUARANTINE_RULE = "NOT (line_total_matches)"

@dp.view
def order_items_change_feed():
    return (
        spark.readStream.table(bronze_table("bronze_order_items_valid"))
        .filter(col("after").isNotNull())
        .select("after.*", "op", "ts_ms")
    )

dp.create_streaming_table(
    name="silver_order_items_all",
    comment="Order items, unfiltered — every row, good and bad line_total alike. "
    "Not what Gold reads. See silver_order_items for the clean table.",
    expect_all=ORDER_ITEMS_RULES,
)

dp.create_auto_cdc_flow(
    target="silver_order_items_all",
    source="order_items_change_feed",
    keys=["order_item_id"],
    sequence_by="ts_ms",
    stored_as_scd_type=1,
    except_column_list= ["op"]
)

@dp.table(private=True)
def silver_order_items_quality_check():
    return (
        spark.readStream.table("silver_order_items_all")
        .withColumn("line_total_matches", expr(ORDER_ITEMS_RULES["line_total_matches"]))
        .withColumn("is_quarantined", expr(f"NOT ({ORDER_ITEMS_RULES['line_total_matches']})"))
    )

@dp.table(
    name="silver_order_items",
    comment="Order items, current state, line_total verified. This is the table Gold reads — "
    "plain name, same convention as every other Silver table.",
    schema="""
        order_item_id STRING NOT NULL,
        order_id STRING,
        product_id STRING,
        sku STRING,
        quantity LONG,
        unit_price DOUBLE,
        line_total DOUBLE,
        return_requested BOOLEAN,
        return_reason STRING,
        op STRING,
        ts_ms LONG,
        CONSTRAINT pk_silver_order_items PRIMARY KEY (order_item_id)
    """,
)

def silver_order_items():
    return (
        spark.read.table("silver_order_items_quality_check")
        .filter("is_quarantined = false")
        .drop("line_total_matches", "is_quarantined")
    )

@dp.table(
    name="silver_order_items_quarantined",
    comment="Order items where line_total didn't match quantity * unit_price. "
    "Investigation surface — monitored in L16, never hand-edited (see remediation pattern).",
)

def silver_order_items_quarantined():
    return (
        spark.read.table("silver_order_items_quality_check")
        .filter("is_quarantined = true")
        .drop("line_total_matches", "is_quarantined")
    )




