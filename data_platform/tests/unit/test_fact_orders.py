from datetime import date, datetime

import pytest
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from transformation_functions.fact_orders import transform_fact_orders


ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", IntegerType(), False),
        StructField("item_id", IntegerType(), False),
        StructField("order_date", DateType(), False),
        StructField("order_time", StringType(), False),
        StructField("event_timestamp", TimestampType(), False),
        StructField("discount_code", StringType(), True),
    ]
)

MENU_SCHEMA = StructType(
    [
        StructField("menu_item_id", IntegerType(), False),
        StructField("price", DoubleType(), False),
    ]
)

DATES_SCHEMA = StructType(
    [
        StructField("date_key", IntegerType(), False),
    ]
)


@pytest.mark.unit_test
def test_transform_fact_orders(spark):
    orders_data = [
        (
            101,
            5,
            date(2023, 1, 6),
            "6:14:45 PM",
            datetime(2026, 9, 10, 11, 55, 5),
            "SUMMER10",
        )
    ]

    menu_data = [
        (
            5,
            12.50,
        )
    ]

    dates_data = [
        (20230106,)
    ]

    orders_df = spark.createDataFrame(
        orders_data,
        schema=ORDERS_SCHEMA,
    )

    menu_df = spark.createDataFrame(
        menu_data,
        schema=MENU_SCHEMA,
    )

    dates_df = spark.createDataFrame(
        dates_data,
        schema=DATES_SCHEMA,
    )

    result = transform_fact_orders(
        orders_df,
        menu_df,
        dates_df,
    )

    row = result.collect()[0]

    assert row.order_id == 101
    assert row.item_id == 5
    assert row.date_key == 20230106
    assert row.order_date == date(2023, 1, 6)
    assert row.order_time == "6:14:45 PM"
    assert row.discount_code == "SUMMER10"
    assert row.price == 12.50
    assert row.quantity == 1
    assert result.count() == 1

    assert result.columns == [
        "order_id",
        "item_id",
        "date_key",
        "order_date",
        "order_time",
        "event_timestamp",
        "discount_code",
        "price",
        "quantity",
    ]


@pytest.mark.unit_test
def test_transform_fact_orders_keeps_order_when_menu_item_is_missing(spark):
    orders_data = [
        (
            102,
            999,
            date(2023, 1, 6),
            "7:00:00 PM",
            datetime(2026, 9, 10, 11, 56, 0),
            None,
        )
    ]

    menu_data = [
        (
            5,
            12.50,
        )
    ]

    dates_data = [
        (20230106,)
    ]

    orders_df = spark.createDataFrame(
        orders_data,
        schema=ORDERS_SCHEMA,
    )

    menu_df = spark.createDataFrame(
        menu_data,
        schema=MENU_SCHEMA,
    )

    dates_df = spark.createDataFrame(
        dates_data,
        schema=DATES_SCHEMA,
    )

    result = transform_fact_orders(
        orders_df,
        menu_df,
        dates_df,
    )

    row = result.collect()[0]

    assert result.count() == 1
    assert row.order_id == 102
    assert row.item_id == 999
    assert row.date_key == 20230106
    assert row.order_date == date(2023, 1, 6)
    assert row.price is None
    assert row.quantity == 1