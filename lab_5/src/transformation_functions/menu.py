import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def transform_menu(df: DataFrame) -> DataFrame:
    return df.select(
        "menu_item_id",
        "item_name",
        F.col("category").alias("menu_category"),
        "price",
        "file_name",
        "ingest_datetime",
    )