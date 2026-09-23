from pyspark import pipelines as dp
from pyspark.sql.functions import col, sum as _sum, row_number, desc, coalesce, lit
from pyspark.sql import Window

@dp.materialized_view(
    name="gold_product_performance",
    comment="Top sellers and current stockout risk — Merchandising's question in requirements",
)

def gold_product_performance():
    order_items=spark.read.table("silver_order_items")
    products=spark.read.table("silver_products")
    inventory=spark.read.table("silver_inventory")

    sales = (
        order_items
        .groupBy("product_id")
        .agg(
            _sum("quantity").alias("units_sold"),
            _sum("line_total").alias("revenue")
            )
    )

    latest_per_warehouse = (
        inventory
        .withColumn(
            "rn",
            row_number().over(Window.partitionBy("product_id", "warehouse_id").orderBy(desc("snapshot_date"))),
        )
        .filter(col("rn") == 1)
    )

    current_stock = (
        latest_per_warehouse
        .groupBy("product_id")
        .agg(_sum("quantity_available").alias("current_stock"))
    )

    STOCKOUT_THRESHOLD = 20

    return(
        products
        .join(sales, "product_id", "left")
        .join(current_stock, "product_id", "left")
        .withColumn("current_stock", coalesce(col("current_stock"), lit(0)))
        .withColumn("stockout_risk", col("current_stock") < STOCKOUT_THRESHOLD)
        .select("product_id", "product_name", "brand", "units_sold", "revenue", "current_stock", "stockout_risk")
    )

