from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog") # noqa: F821
SILVER_SCHEMA = spark.conf.get("silver_schema") # noqa: F821
GOLD_SCHEMA = spark.conf.get("gold_schema") # noqa: F821


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD_SCHEMA}.dim_menu_item",
    comment="Dimensión de menú — solo versión vigente de cada plato",
    table_properties={"layer": "gold"},
)
def dim_menu_item():
    return (
        spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.menu_silver") # noqa: F821
        .filter(F.col("__END_AT").isNull())
        .select("menu_item_id", "item_name", "menu_category", "price")
    )