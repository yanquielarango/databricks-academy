import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def transform_orders(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("order_details_id").cast("long").alias("order_details_id"),
        F.col("order_id").cast("long").alias("order_id"),
        F.to_date(
            F.col("order_date"),
            "M/d/yy",
        ).alias("order_date"),
        F.col("order_time"),
        F.col("item_id").cast("long").alias("item_id"),
        F.col("event_timestamp").cast("timestamp").alias("event_timestamp"),
        F.col("discount_code"),
        F.current_timestamp().alias("ingest_datetime"),
    )