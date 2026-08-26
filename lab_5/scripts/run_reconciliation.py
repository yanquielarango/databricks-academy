import sys

from databricks.connect import DatabricksSession


CATALOG = "dbr_dev"
SILVER_SCHEMA = "yanquiel_silver"
GOLD_SCHEMA = "yanquiel_gold"

ORDERS_TABLE = "orders_silver"
FACT_ORDERS_TABLE = "fact_orders"


def main():
    spark = (
        DatabricksSession.builder
        .serverless()
        .profile("default")
        .getOrCreate()
    )

    silver_table = f"{CATALOG}.{SILVER_SCHEMA}.{ORDERS_TABLE}"
    gold_table = f"{CATALOG}.{GOLD_SCHEMA}.{FACT_ORDERS_TABLE}"

    print("\nRunning reconciliation checks...")
    print(f"Silver: {silver_table}")
    print(f"Gold:   {gold_table}")

    orders_df = spark.table(silver_table)
    fact_orders_df = spark.table(gold_table)

 
    silver_count = orders_df.count()
    gold_count = fact_orders_df.count()

    print("\nRow-count reconciliation")
    print(f"Silver orders: {silver_count}")
    print(f"Gold orders:   {gold_count}")

    row_count_matches = silver_count == gold_count

  
    gold_quantity = (
        fact_orders_df
        .selectExpr("COALESCE(SUM(quantity), 0) AS total_quantity")
        .collect()[0]
        .total_quantity
    )

    print("\nAggregate reconciliation")
    print(f"Silver rows:        {silver_count}")
    print(f"Gold SUM(quantity): {gold_quantity}")

    quantity_matches = silver_count == gold_quantity

 
    if not row_count_matches:
        print(
            "\nRECONCILIATION FAILED: "
            "Silver and Gold row counts do not match."
        )
        sys.exit(1)

    if not quantity_matches:
        print(
            "\nRECONCILIATION FAILED: "
            "Gold SUM(quantity) does not match Silver row count."
        )
        sys.exit(1)

    print("\nRECONCILIATION PASSED: Silver and Gold are consistent.")


if __name__ == "__main__":
    main()