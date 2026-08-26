from pyspark import pipelines as dp

from transformation_functions.menu import transform_menu

SILVER_SCHEMA = spark.conf.get("silver_schema")  # noqa: F821


@dp.view(
    name="menu_silver_clean",
    comment="Cleaned menu events ready for SCD2 processing",
)
@dp.expect_or_drop("valid_price", "price > 0")
@dp.expect_or_drop("valid_menu_item_id", "menu_item_id IS NOT NULL")
def menu_silver_clean():
    df = dp.read_stream("menu_bronze")

    return transform_menu(df)


dp.create_streaming_table(
    name=f"{SILVER_SCHEMA}.menu_silver"
)

dp.create_auto_cdc_flow(
    target=f"{SILVER_SCHEMA}.menu_silver",
    source="menu_silver_clean",
    keys=["menu_item_id"],
    sequence_by="ingest_datetime",
    stored_as_scd_type=2,
)