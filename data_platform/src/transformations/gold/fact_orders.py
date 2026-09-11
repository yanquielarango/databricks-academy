from pyspark import pipelines as dp

from transformation_functions.fact_orders import transform_fact_orders


CATALOG = spark.conf.get("catalog")  # noqa: F821
SILVER_SCHEMA = spark.conf.get("silver_schema")  # noqa: F821
GOLD_SCHEMA = spark.conf.get("gold_schema")  # noqa: F821


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD_SCHEMA}.fact_orders",
    comment="Fact table of orders, one record per order line",
    table_properties={
        "quality": "gold",
        "layer": "gold",
    },
)
def fact_orders():
    orders = dp.read(f"{SILVER_SCHEMA}.orders_silver")
    menu = dp.read(f"{GOLD_SCHEMA}.dim_menu_item")
    dates = dp.read(f"{GOLD_SCHEMA}.dim_date")

    return transform_fact_orders(
        orders,
        menu,
        dates,
    )