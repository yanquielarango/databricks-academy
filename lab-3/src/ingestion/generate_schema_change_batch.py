# src/ingestion/generate_schema_change_batch.py
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

from upload_to_azure import get_filesystem_client, upload_file

SOURCE = "data/order_details.csv"
OUTPUT_FOLDER = Path("landing_local/orders/batch_012")
NEW_ORDER_ID_OFFSET = 200000


def main():
    df = pd.read_csv(SOURCE)

    sample = df.sample(10).copy()
    sample["order_details_id"] = sample["order_details_id"] + NEW_ORDER_ID_OFFSET

    # columna nueva que Auto Loader nunca ha visto
    sample["discount_code"] = "PROMO10"

    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)
    file_path = OUTPUT_FOLDER / "order_details_new_column.csv"
    sample.to_csv(file_path, index=False)
    print(f"Generado: {file_path} ({len(sample)} filas, con columna discount_code)")

    filesystem_client = get_filesystem_client()
    upload_file(filesystem_client, file_path)


if __name__ == "__main__":
    main()