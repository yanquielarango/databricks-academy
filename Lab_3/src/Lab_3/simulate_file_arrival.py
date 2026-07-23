import time
from pathlib import Path
import shutil


SOURCE_FOLDER = Path("/Volumes/dbr_dev/yanquiel_bronze/staging/orders")
TARGET_FOLDER = Path("/Volumes/dbr_dev/yanquiel_bronze/landing/orders")
SECONDS_BETWEEN_BATCHES = 5

TARGET_FOLDER.mkdir(parents=True, exist_ok=True)

batch_folders = sorted(SOURCE_FOLDER.glob("batch_*"))

for batch_folder in batch_folders:

    target_batch = TARGET_FOLDER / batch_folder.name
    target_batch.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(batch_folder.glob("*.csv"))

    for csv_file in csv_files:
        shutil.copy2(csv_file, target_batch / csv_file.name)

    print(f"Uploaded {batch_folder.name} ({len(csv_files)} files)")

    time.sleep(SECONDS_BETWEEN_BATCHES)

print("All batches uploaded")