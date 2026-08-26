import sys
from pathlib import Path

import yaml

from databricks.connect import DatabricksSession
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient


ORDERS_CHECKS_PATH = Path("dq/silver/orders.yml")
MENU_CHECKS_PATH = Path("dq/silver/menu.yml")

CATALOG = "dbr_dev"
SILVER_SCHEMA = "yanquiel_silver"
GOLD_SCHEMA = "yanquiel_gold"

ORDERS_TABLE = "orders_silver"
MENU_TABLE = "menu_silver"


def load_checks(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def main():
    spark = (
        DatabricksSession.builder
        .serverless()
        .profile("default")
        .getOrCreate()
    )

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    orders_table_name = f"{CATALOG}.{SILVER_SCHEMA}.{ORDERS_TABLE}"
    menu_silver_table_name = f"{CATALOG}.{SILVER_SCHEMA}.{MENU_TABLE}"
    menu_dimension_table_name = f"{CATALOG}.{GOLD_SCHEMA}.dim_menu_item"

    dq_engine = DQEngine(
        WorkspaceClient(profile="default")
    )

    # =========================================================
    # ORDERS DQX
    # =========================================================

    print(f"\nRunning DQX checks on: {orders_table_name}")

    orders_df = spark.table(orders_table_name)
    orders_checks = load_checks(ORDERS_CHECKS_PATH)

    dq_engine.validate_checks(orders_checks)

    orders_valid_df, orders_invalid_df = (
        dq_engine.apply_checks_by_metadata_and_split(
            orders_df,
            orders_checks,
        )
    )

    orders_total_count = orders_df.count()
    orders_valid_count = orders_valid_df.count()
    orders_invalid_count = orders_invalid_df.count()

    print(f"Total rows:   {orders_total_count}")
    print(f"Valid rows:   {orders_valid_count}")
    print(f"Invalid rows: {orders_invalid_count}")

    # =========================================================
    # ORDERS CONSISTENCY
    # orders_silver.item_id must exist in dim_menu_item
    # =========================================================

    menu_dimension_df = spark.table(menu_dimension_table_name)

    order_items = (
        orders_df
        .select("item_id")
        .distinct()
    )

    menu_dimension_items = (
        menu_dimension_df
        .select("menu_item_id")
        .distinct()
    )

    missing_menu_items = (
        order_items
        .join(
            menu_dimension_items,
            order_items.item_id == menu_dimension_items.menu_item_id,
            "left_anti",
        )
    )

    missing_menu_count = missing_menu_items.count()

    print(f"Missing menu references: {missing_menu_count}")

    # =========================================================
    # MENU DQX
    # =========================================================

    print(f"\nRunning DQX checks on: {menu_silver_table_name}")

    menu_silver_df = spark.table(menu_silver_table_name)
    menu_checks = load_checks(MENU_CHECKS_PATH)

    dq_engine.validate_checks(menu_checks)

    menu_valid_df, menu_invalid_df = (
        dq_engine.apply_checks_by_metadata_and_split(
            menu_silver_df,
            menu_checks,
        )
    )

    menu_total_count = menu_silver_df.count()
    menu_valid_count = menu_valid_df.count()
    menu_invalid_count = menu_invalid_df.count()

    print(f"Total rows:   {menu_total_count}")
    print(f"Valid rows:   {menu_valid_count}")
    print(f"Invalid rows: {menu_invalid_count}")

    # =========================================================
    # MENU SCD2 CONSISTENCY
    # Only one active version per menu_item_id
    # =========================================================

    active_duplicates = (
        menu_silver_df
        .filter("__END_AT IS NULL")
        .groupBy("menu_item_id")
        .count()
        .filter("count > 1")
    )

    active_duplicate_count = active_duplicates.count()

    print(
        "Menu items with multiple active versions: "
        f"{active_duplicate_count}"
    )

    # =========================================================
    # CI GATE
    # =========================================================

    if orders_invalid_count > 0:
        print("\nORDERS DQX FAILED: data quality errors found.")
        orders_invalid_df.show(truncate=False)
        sys.exit(1)

    if missing_menu_count > 0:
        print(
            "\nORDERS CONSISTENCY FAILED: some item_id values "
            "do not exist in dim_menu_item."
        )
        missing_menu_items.show(truncate=False)
        sys.exit(1)

    if menu_invalid_count > 0:
        print("\nMENU DQX FAILED: data quality errors found.")
        menu_invalid_df.show(truncate=False)
        sys.exit(1)

    if active_duplicate_count > 0:
        print(
            "\nMENU SCD2 CONSISTENCY FAILED: "
            "multiple active versions found."
        )
        active_duplicates.show(truncate=False)
        sys.exit(1)

    print("\nDQX PASSED: all data quality checks passed.")


if __name__ == "__main__":
    main()