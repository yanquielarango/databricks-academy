from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType


KNOWLEDGE_DIR = Path("knowledge")


DOCUMENT_SCHEMA = StructType(
    [
        StructField("document_id", StringType(), False),
        StructField("document_name", StringType(), False),
        StructField("source_path", StringType(), False),
        StructField("content", StringType(), False),
    ]
)


def load_markdown_documents(spark: SparkSession, knowledge_dir: Path = KNOWLEDGE_DIR) -> DataFrame:
    rows = []

    for path in knowledge_dir.glob("*.md"):
        content = path.read_text(encoding="utf-8")
        rows.append((path.stem, path.name, str(path), content))

    return spark.createDataFrame(rows, schema=DOCUMENT_SCHEMA)


def clean_documents(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("content", F.trim(F.col("content")))
        .filter(F.length(F.col("content")) > 0)
    )