import pytest
import pyspark.sql.functions as F

from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from transformation_functions.orders import transform_orders


INPUT_SCHEMA = StructType(
    [
        StructField("raw_json", StringType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True),
    ]
)


@pytest.mark.unit_test
def test_transform_orders(spark):
    input_data = [
        (
            '{"order_id":"101","item_id":"5","event_timestamp":"2026-08-26T10:30:00Z","discount_code":"SUMMER10"}',
            "orders_event",
            0,
            123,
        )
    ]

    df = spark.createDataFrame(
        input_data,
        schema=INPUT_SCHEMA,
    )

    result = transform_orders(df)

    row = result.collect()[0]

    assert row.order_id == 101
    assert row.item_id == 5
    assert row.discount_code == "SUMMER10"
    assert row.topic == "orders_event"
    assert row.partition == 0
    assert row.offset == 123
    assert row.ingest_datetime is not None

    timestamp_value = (
        result
        .select(
            F.date_format(
                F.col("event_timestamp"),
                "yyyy-MM-dd HH:mm:ss",
            ).alias("event_timestamp")
        )
        .collect()[0]
        .event_timestamp
    )

    assert timestamp_value == "2026-08-26 10:30:00"

    schema = result.schema

    assert isinstance(schema["order_id"].dataType, IntegerType)
    assert isinstance(schema["item_id"].dataType, IntegerType)
    assert isinstance(schema["event_timestamp"].dataType, TimestampType)
    assert isinstance(schema["discount_code"].dataType, StringType)
    assert isinstance(schema["ingest_datetime"].dataType, TimestampType)