# Databricks notebook source

from datetime import datetime, timezone

from pyspark.sql import SparkSession


spark = SparkSession.getActiveSession()

if spark is None:
    raise RuntimeError("Spark session is not available")


print("Lab 9 - Platform Automation")
print("===========================")

print(f"Execution time: {datetime.now(timezone.utc).isoformat()}")

print("\nSpark version:")
print(spark.version)

print("\nCurrent user:")
spark.sql("SELECT current_user()").show(truncate=False)

print("\nCurrent catalog:")
spark.sql("SELECT current_catalog()").show(truncate=False)

print("\nTesting Spark execution...")

test_df = spark.range(1, 11)

row_count = test_df.count()

print(f"Rows processed: {row_count}")

if row_count != 10:
    raise RuntimeError(
        f"Expected 10 rows but received {row_count}"
    )

print("\nLAB 9 PLATFORM CHECK PASSED")