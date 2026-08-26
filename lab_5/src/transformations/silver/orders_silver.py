from pyspark import pipelines as dp
from pyspark.sql import functions as F

from transformation_functions.orders import transform_orders


SILVER_SCHEMA = spark.conf.get("silver_schema")  # noqa: F821


# ------------------------------------------------------------------
# Parsed orders
# Shared source for valid and quarantined records
# ------------------------------------------------------------------

@dp.view(
    name="orders_parsed",
    comment="Parsed orders before data quality routing",
)
def orders_parsed():
    df = dp.read_stream("orders_bronze")

    return transform_orders(df)


# ------------------------------------------------------------------
# Valid orders
# ------------------------------------------------------------------

@dp.table(
    name=f"{SILVER_SCHEMA}.orders_silver",
    comment="Valid parsed orders ready for analytics",
)
@dp.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dp.expect_or_drop("valid_item_id", "item_id IS NOT NULL")
@dp.expect_or_drop(
    "valid_event_timestamp",
    "event_timestamp IS NOT NULL",
)
def orders_silver():
    return dp.read_stream("orders_parsed")


# ------------------------------------------------------------------
# Quarantine
# ------------------------------------------------------------------

@dp.table(
    name=f"{SILVER_SCHEMA}.orders_quarantine",
    comment="Orders rejected by Silver data quality rules",
    table_properties={
        "quality": "quarantine",
        "layer": "silver",
    },
)
def orders_quarantine():
    return (
        dp.read_stream("orders_parsed")
        .filter(
            """
            order_id IS NULL
            OR item_id IS NULL
            OR event_timestamp IS NULL
            """
        )
        .withColumn(
            "dq_reason",
            F.when(
                F.col("order_id").isNull(),
                F.lit("missing_order_id"),
            )
            .when(
                F.col("item_id").isNull(),
                F.lit("missing_item_id"),
            )
            .when(
                F.col("event_timestamp").isNull(),
                F.lit("missing_or_invalid_event_timestamp"),
            )
            .otherwise(
                F.lit("unknown_data_quality_error"),
            ),
        )
    )