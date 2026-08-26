from datetime import datetime

import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from transformation_functions.menu import transform_menu

INPUT_SCHEMA = StructType(
    [
        StructField("menu_item_id", IntegerType(), True),
        StructField("item_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("file_name", StringType(), True),
        StructField("ingest_datetime", TimestampType(), True),
    ]
)


@pytest.mark.unit_test
def test_transform_menu(spark):
    input_data = [
        (
            101,
            "Cheeseburger",
            "Burgers",
            12.50,
            "menu.csv",
            datetime(2026, 8, 26, 10, 30, 0),
        )
    ]

    df = spark.createDataFrame(
        input_data,
        schema=INPUT_SCHEMA,
    )

    result = transform_menu(df)

    row = result.collect()[0]

    assert row.menu_item_id == 101
    assert row.item_name == "Cheeseburger"
    assert row.menu_category == "Burgers"
    assert row.price == 12.50
    assert row.file_name == "menu.csv"
    assert row.ingest_datetime is not None

    assert result.columns == [
        "menu_item_id",
        "item_name",
        "menu_category",
        "price",
        "file_name",
        "ingest_datetime",
    ]