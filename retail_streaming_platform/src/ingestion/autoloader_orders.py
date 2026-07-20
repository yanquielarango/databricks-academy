# src/ingestion/autoloader_orders.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

STORAGE_ACCOUNT = "dlspl26databricks"   # <- corregido
CONTAINER = "yanquiel"
BASE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/landing"

LANDING_PATH = f"{BASE_PATH}/orders/"
SCHEMA_LOCATION = f"{BASE_PATH}/_checkpoints/orders/schema"
CHECKPOINT_LOCATION = f"{BASE_PATH}/_checkpoints/orders/checkpoint"

CATALOG = "dbr_dev"
SCHEMA = "yanquiel"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.brz_order_details"


def main():
    spark = SparkSession.builder.getOrCreate()

    df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        .option("cloudFiles.inferColumnTypes", "true")
        .load(LANDING_PATH)
    )

    df_enriched = (df
        .withColumn("_ingest_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_name"))
    )

    query = (df_enriched.writeStream
        .format("delta")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(availableNow=True)
        .toTable(BRONZE_TABLE)
    )

    query.awaitTermination()
    print(f"Ingestión completa hacia {BRONZE_TABLE}")


if __name__ == "__main__":
    main()