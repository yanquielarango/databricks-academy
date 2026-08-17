from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.types import MapType, StringType


SILVER_SCHEMA = spark.conf.get("silver_schema")  # noqa: F821

@dp.table(
    name=f"{SILVER_SCHEMA}.orders_silver",
    comment="Parsed orders events ready for analytics",
)
@dp.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dp.expect_or_drop("valid_item_id", "item_id IS NOT NULL")
def orders_silver():
    df_parsed = (
        dp.read_stream("orders_bronze")
        .withColumn(
            "event",
            F.from_json(F.col("raw_json"), MapType(StringType(), StringType()))
        )
    )

    return (
        df_parsed
        .select(
            F.col("event")["order_id"].cast("int").alias("order_id"),
            F.col("event")["item_id"].cast("int").alias("item_id"),
            F.col("event")["event_timestamp"].cast("timestamp").alias("event_timestamp"),
            F.col("event")["discount_code"].alias("discount_code"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.current_timestamp().alias("ingest_datetime"),
        )
    )