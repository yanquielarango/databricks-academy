# src/ingestion/generate_new_batch.py
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

from upload_to_azure import get_filesystem_client, upload_file  # reutilizamos lo que ya funciona

SOURCE = "data/order_details.csv"
OUTPUT_FOLDER = Path("landing_local/orders/batch_011")
NEW_ORDER_ID_OFFSET = 100000  # para que los ids no choquen con los originales


def main():
    df = pd.read_csv(SOURCE)

    # tomamos 20 filas al azar y las tratamos como "pedidos nuevos"
    sample = df.sample(20).copy()
    sample["order_details_id"] = sample["order_details_id"] + NEW_ORDER_ID_OFFSET
    sample["batch_id"] = 11
    sample["source_file"] = SOURCE
    sample["created_at"] = datetime.now(timezone.utc)

    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)
    file_path = OUTPUT_FOLDER / "order_details_new.csv"
    sample.to_csv(file_path, index=False)
    print(f"Generado: {file_path} ({len(sample)} filas)")

    # subimos directo, sin esperar al script de upload completo
    filesystem_client = get_filesystem_client()
    upload_file(filesystem_client, file_path)


if __name__ == "__main__":
    main()