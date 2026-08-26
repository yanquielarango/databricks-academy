import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import MapType, StringType


def transform_orders(df: DataFrame) -> DataFrame:

    df_parsed = df.withColumn(
        "event",
        F.from_json(
            F.col("raw_json"),
            MapType(StringType(), StringType()),
        ),
    )

    return df_parsed.select(
        F.col("event")["order_id"].cast("int").alias("order_id"),
        F.col("event")["item_id"].cast("int").alias("item_id"),
        F.col("event")["event_timestamp"].cast("timestamp").alias("event_timestamp"),
        F.col("event")["discount_code"].alias("discount_code"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.current_timestamp().alias("ingest_datetime"),
    )