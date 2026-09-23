from pyspark import pipelines as dp
from gold_revenue_logic import compute_daily_revenue

@dp.materialized_view(
    name="gold_daily_revenue",
    comment="Revenue by day, category, and region — gross, discount, and net reported "
    "separately. The trustworthy number Finance asked for in L1."
)

def gold_daily_revenue():

    return compute_daily_revenue(
        order_items_df=spark.read.table("silver_order_items"),
        orders_df=spark.read.table("silver_orders"),
        products_df=spark.read.table("silver_products"),
        categories_df=spark.read.table("silver_categories")
    )