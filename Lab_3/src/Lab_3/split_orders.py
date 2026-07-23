import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone

SOURCE = "/Volumes/dbr_dev/yanquiel_bronze/staging/data/order_details.csv"
OUTPUT_FOLDER = Path("/Volumes/dbr_dev/yanquiel_bronze/staging/orders")

TOTAL_FILES = 1000
FILES_PER_BATCH = 100

OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

df = pd.read_csv(SOURCE)
print(f"Total rows: {len(df)}")

row_index_groups = np.array_split(np.arange(len(df)), TOTAL_FILES)

for i, row_indices in enumerate(row_index_groups):
    batch_number = i // FILES_PER_BATCH + 1

    batch_folder = OUTPUT_FOLDER / f"batch_{batch_number:03d}"
    batch_folder.mkdir(parents=True, exist_ok=True)

    chunk = df.iloc[row_indices].copy()

    chunk["batch_id"] = batch_number
    chunk["source_file"] = SOURCE
    chunk["created_at"] = datetime.now(timezone.utc)

    chunk.to_csv(batch_folder / f"order_details_{i:04d}.csv", index=False)

print(f"Created {TOTAL_FILES} files inside {OUTPUT_FOLDER}")