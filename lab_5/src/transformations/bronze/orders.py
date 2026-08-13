from pyspark import pipelines as dp
import pyspark.sql.functions as F

BOOTSTRAP_SERVERS = "pkc-56d1g.eastus.azure.confluent.cloud:9092"
TOPIC_NAME = "orders_event"

BRONZE_SCHEMA = spark.conf.get("bronze_schema")  # noqa: F821

@dp.table(
    name=f"{BRONZE_SCHEMA}.orders_bronze",
    comment="Orders raw data from Confluent Kafka",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "kafka",
        "delta.enableChangeDataFeed": "true",
    },
)
def orders_bronze():
    api_key = dbutils.secrets.get(scope="confluent-scope", key="api-key")  # noqa: F821
    api_secret = dbutils.secrets.get(scope="confluent-scope", key="api-secret")  # noqa: F821

    kafka_options = {
        "kafka.bootstrap.servers": BOOTSTRAP_SERVERS,
        "subscribe": TOPIC_NAME,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": (
            "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{api_key}" password="{api_secret}";'
        ),
        "startingOffsets": "earliest",
    }

    df_raw = spark.readStream.format("kafka").options(**kafka_options).load()  # noqa: F821

    return (
        df_raw
        .withColumn("raw_json", F.col("value").cast("string"))
        .withColumn("ingest_datetime", F.current_timestamp())
        .select("raw_json", "topic", "partition", "offset", "timestamp", "ingest_datetime")
    )