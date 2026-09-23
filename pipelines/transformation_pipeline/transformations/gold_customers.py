from pyspark import pipelines as dp
from pyspark.sql.functions import col, sum as _sum, count, max as _max, datediff, current_date, to_date

@dp.materialized_view(
    name="gold_customer_360",
    comment="Lifetime value, order frequency, and a descriptive churn signal — Marketing's question from requirement."
)

def gold_customer_360():
    customers_current = spark.read.table("silver_customers").filter(col("__END_AT").isNull())
    orders_current = spark.read.table("silver_orders").filter(col("__END_AT").isNull())

    order_summary = (
        orders_current
        .groupBy("customer_id")
        .agg(
            _sum("total_amount").alias("lifetime_value"),
            count("order_id").alias("order_count"),
            _max("order_date").alias("last_order_date")
        )
    )

    return(
        customers_current
        .join(order_summary,"customer_id","left")
        .withColumn(
            "days_since_last_order",
            datediff(current_date(), to_date(col("last_order_date")))
        )
        .select(
            "customer_id", "first_name", "last_name", "loyalty_tier",
            "lifetime_value", "order_count", "last_order_date", "days_since_last_order"
        )
    )