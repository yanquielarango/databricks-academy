from pyspark import pipelines as dp
import pyspark.sql.functions as F

SILVER_SCHEMA = spark.conf.get("silver_schema")  # noqa: F821


@dp.view(
    name="menu_silver_clean",
    comment="Cleaned menu events ready for SCD2 processing"
)
@dp.expect_or_drop("valid_price", "price > 0")
@dp.expect_or_drop("valid_menu_item_id", "menu_item_id IS NOT NULL")
def menu_silver_clean():
    return (
        dp.read_stream("menu_bronze")   
        .select(
            "menu_item_id",
            "item_name",
            F.col("category").alias("menu_category"),
            "price",
            "file_name",
            "ingest_datetime",
        )
    )


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