from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)

CATALOG = spark.conf.get("catalog")  # noqa: F821
BRONZE_SCHEMA = spark.conf.get("bronze_schema")  # noqa: F821

SOURCE_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/landing/menu/"

MENU_SCHEMA = StructType([
    StructField("menu_item_id", IntegerType(), True),
    StructField("item_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("_corrupt_record", StringType(), True),
])


@dp.table(
    name=f"{BRONZE_SCHEMA}.menu_bronze",
    comment="Menu raw data processing",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def menu_bronze():
    df = (
        spark.readStream  # noqa: F821
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(MENU_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .load(SOURCE_PATH)
    )

    return (
        df.withColumn("file_name", F.col("_metadata.file_path"))
        .withColumn("ingest_datetime", F.current_timestamp())
    )