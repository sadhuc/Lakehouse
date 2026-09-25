from datetime import date,datetime

import pytest
from pyspark.testing.utils import assertDataFrameEqual

from gold_revenue_logic import compute_daily_revenue
from gold_product_logic import compute_product_performance
from gold_funnel_logic import compute_funnel_analysis
from gold_fulfillment_logic import compute_fulfillment_health
from gold_customers_logic import compute_customer_360

# ---------------------------------------------------------------------------
# gold_daily_revenue
# ---------------------------------------------------------------------------

def test_daily_revenue_allocates_discount_proportionally_and_filters_scd2(local_spark):
    categories_df = local_spark.createDataFrame(
        [("CAT1","Formal"), ("CAT2","Sports")],
        schema = "category_id string, category_name string"
    )

    products_df = local_spark.createDataFrame(
        [("P1", "CAT1"), ("P2", "CAT2")],
        schema="product_id string, category_id string"
    )

    orders_df = local_spark.createDataFrame(
        [
            ("O1", "2026-09-24", "CA", 10.0, None),
            ("O1", "2026-09-24", "CA", 999.0, "2026-09-23T00:00:00"),
            ("O2", "2026-07-10", "NY", None, None)
        ],
        schema="order_id string, order_date string, shipping_state string, discount_amount double, __END_AT string"
    )

    order_items_df = local_spark.createDataFrame(
        [
            ("I1","O1","P1",60.0),
            ("I2","O1","P2",40.0),
            ("I3","O2","P1",50.0)

        ],
        schema = "order_item_id string, order_id string, product_id string, line_total double"
    )

    result = compute_daily_revenue(order_items_df,orders_df,products_df,categories_df)

    expected = local_spark.createDataFrame(
        [
            (date(2026,9,24), "Formal", "CA", 60.0, 6.0, 54.0),
            (date(2026,9,24), "Sports", "CA", 40.0, 4.0, 36.0),
            (date(2026,7,10), "Formal", "NY", 50.0, 0.0, 50.0)

        ],
        schema = "revenue_date date, category_name string, region string, gross_revenue double, total_discount double, net_revenue double"
    )

    assertDataFrameEqual(result,expected,checkRowOrder=False,rtol=1e-4)

def test_daily_revenue_guards_against_zero_order_subtotal(local_spark):    
    categories_df = local_spark.createDataFrame(
        [("CAT1", "Formal")], schema="category_id string, category_name string"
    )
    products_df = local_spark.createDataFrame(
        [("P1", "CAT1")], schema="product_id string, category_id string"
    )
    orders_df = local_spark.createDataFrame(
        [("O1", "2026-07-10", "CA", 5.0, None)],
        schema="order_id string, order_date string, shipping_state string, discount_amount double, __END_AT string",
    )
    order_items_df = local_spark.createDataFrame(
        [("I1", "O1", "P1", 0.0)],
        schema="order_item_id string, order_id string, product_id string, line_total double",
    )

    result = compute_daily_revenue(order_items_df, orders_df, products_df, categories_df)

    expected = local_spark.createDataFrame(
        [(date(2026, 7, 10), "Formal", "CA", 0.0, 0.0, 0.0)],
        schema="revenue_date date, category_name string, region string, "
        "gross_revenue double, total_discount double, net_revenue double",
    )

    assertDataFrameEqual(result, expected, checkRowOrder=False, rtol=1e-4)

# ---------------------------------------------------------------------------
# gold_product_performance
# ---------------------------------------------------------------------------

def test_product_performance_latest_snapshot_and_null_safety(local_spark):
    products_df = local_spark.createDataFrame(
        [
            ("P1","Product One", "Brand A"),
            ("P2", "Product Two", "Brand B"),
            ("P3", "Product Three", "Brand C")
        ],
        schema = "product_id string, product_name string, brand string"
    )

    order_items_df = local_spark.createDataFrame(
        [("I1", "O1", "P1", 2, 30.0), ("I2", "O2", "P3", 1, 90.0)],
        schema = "order_item_id string, order_id string, product_id string, quantity int, line_total double"
    )

    inventory_df = local_spark.createDataFrame(
        [
            ("P1","WH1",date(2026,9,24),5),
            ("P1","WH1",date(2026,9,20),100),
            ("P1","WH2",date(2026,9,24),10),
            ("P3","WH1",date(2026,9,24),100)
        ],
        schema = "product_id string, warehouse_id string, snapshot_date date, quantity_available int"
    )

    stockout_threshold = 20

    result = compute_product_performance(order_items_df,products_df,inventory_df)
    expected = local_spark.createDataFrame(
        [
            ("P1","Product One","Brand A", 2, 30.0, 15, True),
            ("P2", "Product Two", "Brand B", None, None, 0, True),
            ("P3", "Product Three", "Brand C", 1, 90.0, 100, False)

        ],
        schema = "product_id string, product_name string, brand string, units_sold long, revenue double, current_stock long, stockout_risk boolean"
    )

    assertDataFrameEqual(result, expected, checkRowOrder=False, rtol=1e-4)

# ---------------------------------------------------------------------------
# gold_customer_360
# ---------------------------------------------------------------------------

def test_customer_360_is_deterministic_given_a_fixed_as_of_date(local_spark):
    customers_df = local_spark.createDataFrame(
        [
            ("C1","David","G","bronze","2026-07-08"),
            ("C1","David","G","silver","2026-08-20"),
            ("C1","David","G","gold",None)
        ],
        schema = "customer_id string, first_name string, last_name string, loyalty_tier string, __END_AT string"
    )

    orders_df = local_spark.createDataFrame(
        [
            ("O1","C1",100.0,"2026-07-20",None),
            ("O2","C1",200.0,"2026-09-23",None)

        ],
        schema = "order_id string, customer_id string, total_amount double, order_date string, __END_AT string"
    )

    result = compute_customer_360(customers_df,orders_df,as_of_date=date(2026, 9, 30))

    expected = local_spark.createDataFrame(
        [
            ("C1","David","G","gold",300.0,2,"2026-09-23",7)
        ],
        schema = "customer_id string, first_name string, last_name string, loyalty_tier string, lifetime_value double, order_count long, last_order_date string, days_since_last_order int"
    )

    assertDataFrameEqual(result, expected, checkRowOrder=False, rtol=1e-4)

# ---------------------------------------------------------------------------
# gold_funnel_analysis
# ---------------------------------------------------------------------------

def test_funnel_analysis_counts_anonymous_sessions(local_spark):
    events_df = local_spark.createDataFrame(
        [
            ("S1","affiliate","add_to_cart"),
            ("S1","affiliate","purchase"),
            ("S2","google","product_view"),
            ("S3","direct","purchase")
        ],
        schema= "session_id string, referrer string, event_type string"
    )

    result = compute_funnel_analysis(events_df)

    expected = local_spark.createDataFrame(
        [
         ("affiliate", 1, 1, 1.0),
         ("google", 1, 0, 0.0),
         ("direct", 1, 1, 1.0)
        ],
        schema="referrer string,total_sessions long,converted_sessions long,conversion_rate double"
    )

    assertDataFrameEqual(result, expected, checkRowOrder=False, rtol=1e-4)

# ---------------------------------------------------------------------------
# gold_fulfillment_health
# ---------------------------------------------------------------------------

def test_fulfillment_health_sla_boundary_is_inclusive(local_spark):
    orders_df = local_spark.createDataFrame(
        [
            ("O1", "delivered", date(2026,8,23), date(2026,8,26), "CA", None),
            ("O2", "delivered", date(2026,8,23), date(2026,9,5), "CA", None),
            ("O3", "pending", date(2026,9,3), date(2026,9,3), "CA", None)
        ],
        schema = "order_id string,  order_status string, order_date date, updated_at date,  shipping_state string, __END_AT string"
    )

    SLA_DAYS = 5
    result = compute_fulfillment_health(orders_df)

    expected = local_spark.createDataFrame(
        [(date(2026, 8, 23), "CA", 2, 1, 0.5)],
        schema = "order_date date, region string, delivered_orders long, within_sla_orders long, sla_compliance_rate double"
    )

    assertDataFrameEqual(result, expected, checkRowOrder=False, rtol=1e-4)


