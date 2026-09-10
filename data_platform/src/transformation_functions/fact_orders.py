from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_fact_orders(
    orders: DataFrame,
    menu: DataFrame,
    dates: DataFrame,
) -> DataFrame:
    return (
        orders
        .withColumn(
            "date_key",
            F.date_format(
                F.col("order_date"),
                "yyyyMMdd",
            ).cast("int"),
        )
        .withColumn(
            "quantity",
            F.lit(1),
        )
        .join(
            menu,
            orders.item_id == menu.menu_item_id,
            "left",
        )
        .join(
            dates,
            "date_key",
            "left",
        )
        .select(
            "order_id",
            "item_id",
            "date_key",
            "order_date",
            "order_time",
            "event_timestamp",
            "discount_code",
            "price",
            "quantity",
        )
    )