from pyspark import pipelines as dp
from gold_product_logic import compute_product_performance

@dp.materialized_view(
    name="gold_product_performance",
    comment="Top sellers and current stockout risk — Merchandising's question in requirements",
)

def gold_product_performance():

    return(
        compute_product_performance(
            order_items_df=spark.read.table("silver_order_items"),
            products_df=spark.read.table("silver_products"),
            inventory_df=spark.read.table("silver_inventory")
        )
    )