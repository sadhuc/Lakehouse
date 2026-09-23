from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, sum as _sum, when, datediff, to_date

SLA_DAYS = 5

@dp.materialized_view(
    name="gold_fulfillment_health",
    comment="Order fulfillment SLA compliance by day and region — Operations' question from L1. "
    "Order-timing metric only; not joined to inventory (orders has no warehouse_id)."
)
def gold_fulfillment_health():
    orders_current = spark.read.table("silver_orders").filter(col("__END_AT").isNull())
    delivered = (
        orders_current
        .filter(col("order_status") == "delivered")
        .withColumn(
            "days_to_deliver",
            datediff(to_date(col("updated_at")), to_date(col("order_date"))),
        )
        .withColumn("within_sla", col("days_to_deliver") <= SLA_DAYS)
    )

    return(
        delivered
        .groupBy(
            to_date(col("order_date")).alias("order_date"),
            col("shipping_state").alias("region")
        )
        .agg(
            count("order_id").alias("delivered_orders"),
            _sum(when(col("within_sla"),1).otherwise(0)).alias("within_sla_orders")
        )
        .withColumn("sla_compliance_rate",col("within_sla_orders") / col("delivered_orders"))
    )