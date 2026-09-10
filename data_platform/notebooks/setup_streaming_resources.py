# Databricks notebook source

from databricks.sdk.runtime import dbutils, spark


dbutils.widgets.text("catalog", "")
dbutils.widgets.text("bronze_schema", "")
dbutils.widgets.text("zerobus_principal", "")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
zerobus_principal = dbutils.widgets.get("zerobus_principal")

table_name = f"{catalog}.{bronze_schema}.orders_bronze"

print("Setting up Zerobus streaming resources")
print(f"Catalog: {catalog}")
print(f"Bronze schema: {bronze_schema}")
print(f"Table: {table_name}")
print(f"Zerobus principal: {zerobus_principal}")

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        order_details_id BIGINT,
        order_id BIGINT,
        order_date STRING,
        order_time STRING,
        item_id BIGINT,
        event_timestamp STRING,
        discount_code STRING
    )
    USING DELTA
    """
)

print(f"Table ready: {table_name}")

spark.sql(
    f"""
    GRANT USE CATALOG
    ON CATALOG {catalog}
    TO `{zerobus_principal}`
    """
)

spark.sql(
    f"""
    GRANT USE SCHEMA
    ON SCHEMA {catalog}.{bronze_schema}
    TO `{zerobus_principal}`
    """
)

spark.sql(
    f"""
    GRANT MODIFY, SELECT
    ON TABLE {table_name}
    TO `{zerobus_principal}`
    """
)

print(f"Permissions granted to {zerobus_principal}")
print("ZEROBUS STREAMING SETUP COMPLETED")