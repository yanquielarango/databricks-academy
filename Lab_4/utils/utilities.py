
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType



BRONZE_SCHEMA = "yanquiel_bronze"

SILVER_SCHEMA = "yanquiel_silver"

BRONZE_MENU_TABLE = "brz_menu_items"

BRONZE_ORDER_TABLE = "brz_order_details"



BRONZE_MENU_SCHEMA = StructType([
    StructField("menu_item_id", IntegerType(), True),
    StructField("item_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
])