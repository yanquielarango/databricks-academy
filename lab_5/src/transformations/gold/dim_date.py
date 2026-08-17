from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")  # noqa: F821
GOLD_SCHEMA = spark.conf.get("gold_schema") # noqa: F821
start_date = spark.conf.get("start_date") # noqa: F821
end_date = spark.conf.get("end_date") # noqa: F821


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD_SCHEMA}.dim_date",
    comment="Dimensión de calendario para el Gold layer",
    table_properties={
        "layer": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def dim_date():
    df = spark.sql( # noqa: F821
        f"""
        SELECT explode(sequence(
            to_date('{start_date}'),
            to_date('{end_date}'),
            interval 1 day
        )) as full_date
        """
    )
    df = (
        df.withColumn("date_key", F.date_format(F.col("full_date"), "yyyyMMdd").cast("int"))
        .withColumn("year", F.year(F.col("full_date")))
        .withColumn("month", F.month(F.col("full_date")))
        .withColumn("day", F.dayofmonth(F.col("full_date")))
        .withColumn("day_name", F.date_format(F.col("full_date"), "EEEE"))
        .withColumn("day_of_week_num", F.dayofweek(F.col("full_date")))
        .withColumn(
            "is_weekend",
            F.when(F.col("day_of_week_num").isin([1, 7]), True).otherwise(False),
        )
    )
    return df.select(
        "full_date", "date_key", "year", "month", "day",
        "day_name", "is_weekend"
    )