from pyspark import pipelines as dp
from gold_customers_logic import compute_customer_360

@dp.materialized_view(
    name="gold_customer_360",
    comment="Lifetime value, order frequency, and a descriptive churn signal — Marketing's question from requirement."
)

def gold_customer_360():

    return(
        compute_customer_360(
            customers_df = spark.read.table("silver_customers"),
            orders_df = spark.read.table("silver_orders")
        )
    )