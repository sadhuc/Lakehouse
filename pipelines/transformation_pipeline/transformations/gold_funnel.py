from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, sum as _sum, max as _max, when

@dp.materialized_view(
    name="gold_funnel_analysis",
    comment="Conversion rate by channel, including anonymous sessions — Growth's question from L1.",
)

def gold_funnel_analysis():
    events = spark.read.table("silver_clickstream")
    session_outcomes = (
        events
        .groupBy("session_id", "referrer")
        .agg(
            _max(when(col("event_type") == "purchase", 1).otherwise(0)).alias("converted")
        )
    )

    return (
        session_outcomes
        .groupBy("referrer")
        .agg(
            count("session_id").alias("total_sessions"),
            _sum("converted").alias("converted_sessions"),
        )
        .withColumn("conversion_rate", col("converted_sessions") / col("total_sessions"))
    )