from pyspark import pipelines as dp
from gold_funnel_logic import compute_funnel_analysis 


@dp.materialized_view(
    name="gold_funnel_analysis",
    comment="Conversion rate by channel, including anonymous sessions — Growth's question from L1.",
)

def gold_funnel_analysis():
    return(
        compute_funnel_analysis(
            events_df = spark.read.table("silver_clickstream")
        )
    )