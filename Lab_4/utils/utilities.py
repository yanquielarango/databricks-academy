
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType



BRONZE_SCHEMA = "yanquiel_bronze"

SILVER_SCHEMA = "yanquiel_silver"

BRONZE_MENU_TABLE = "brz_menu_items"

BRONZE_ORDER_TABLE = "brz_order_details"

SILVER_MENU_TABLE = "slv_menu_items"

SILVER_ORDER_TABLE = "slv_order_details"


BRONZE_MENU_SCHEMA = StructType([
    StructField("menu_item_id", IntegerType(), True),
    StructField("item_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
])


ORDER_EVENT_SCHEMA = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("event_timestamp", StringType(), True),
])


ORDER_EVENT_SCHEMA_V2 = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("discount_code", StringType(), True),
])