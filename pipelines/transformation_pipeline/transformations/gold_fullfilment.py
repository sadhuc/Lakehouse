from pyspark import pipelines as dp
from gold_fulfillment_logic import compute_fulfillment_health


@dp.materialized_view(
    name="gold_fulfillment_health",
    comment="Order fulfillment SLA compliance by day and region — Operations' question from L1. "
    "Order-timing metric only; not joined to inventory (orders has no warehouse_id)."
)
def gold_fulfillment_health():

    return(
        compute_fulfillment_health(
            orders_df = spark.read.table("silver_orders")
        )
    )