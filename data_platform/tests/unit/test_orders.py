import pytest

from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from transformation_functions.orders import transform_orders


INPUT_SCHEMA = StructType(
    [
        StructField("order_details_id", LongType(), True),
        StructField("order_id", LongType(), True),
        StructField("order_date", StringType(), True),
        StructField("order_time", StringType(), True),
        StructField("item_id", LongType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("discount_code", StringType(), True),
    ]
)


@pytest.mark.unit_test
def test_transform_orders(spark):
    input_data = [
        (
            1001,
            101,
            "1/6/23",
            "6:14:45 PM",
            5,
            "2026-09-10T11:55:05.775872+00:00",
            "SUMMER10",
        )
    ]

    df = spark.createDataFrame(
        input_data,
        schema=INPUT_SCHEMA,
    )

    result = transform_orders(df)

    row = result.collect()[0]

    # Values
    assert row.order_details_id == 1001
    assert row.order_id == 101
    assert row.item_id == 5
    assert str(row.order_date) == "2023-01-06"
    assert row.order_time == "6:14:45 PM"
    assert row.discount_code == "SUMMER10"
    assert row.event_timestamp is not None
    assert row.ingest_datetime is not None

    # Types explicitly produced by the transformation
    schema = result.schema

    assert isinstance(schema["order_details_id"].dataType, LongType)
    assert isinstance(schema["order_id"].dataType, LongType)
    assert isinstance(schema["order_date"].dataType, DateType)
    assert isinstance(schema["item_id"].dataType, LongType)
    assert isinstance(schema["event_timestamp"].dataType, TimestampType)