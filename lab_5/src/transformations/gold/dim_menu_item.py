from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog") # noqa: F821
SILVER_SCHEMA = spark.conf.get("silver_schema") # noqa: F821
GOLD_SCHEMA = spark.conf.get("gold_schema") # noqa: F821


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD_SCHEMA}.dim_menu_item",
    comment="Menu dimension only latest version of each dish",
    table_properties={"layer": "gold"},
)
def dim_menu_item():
    return (
        dp.read(f"{SILVER_SCHEMA}.menu_silver")
        .filter(F.col("__END_AT").isNull())
        .select("menu_item_id", "item_name", "menu_category", "price")
    )