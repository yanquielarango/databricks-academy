from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog") # noqa: F821
SILVER_SCHEMA = spark.conf.get("silver_schema") # noqa: F821
GOLD_SCHEMA = spark.conf.get("gold_schema") # noqa: F821


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD_SCHEMA}.fact_orders",
    comment="Fact table of orders one record per order line",
    table_properties={"layer": "gold"},
)
def fact_orders():
    orders = dp.read(f"{SILVER_SCHEMA}.orders_silver")
   

    return (
        orders
        .withColumn("date_key",F.date_format(F.to_date("event_timestamp"),"yyyyMMdd").cast("int"))
        .withColumn("quantity", F.lit(1))
        .select(
            "order_id",
            "item_id",
            "date_key",
            "event_timestamp",
            "discount_code",
            "price",
            "quantity",
        )
    )