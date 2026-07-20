# src/ingestion/upload_to_azure.py
import os
import time
from pathlib import Path

from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient


load_dotenv()


CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
FILESYSTEM = os.getenv("FILESYSTEM")
LOCAL_FOLDER = Path(os.getenv("LOCAL_FOLDER", "landing_local/orders"))
SECONDS_BETWEEN_BATCHES = 5


def get_filesystem_client():
    service_client = DataLakeServiceClient.from_connection_string(CONNECTION_STRING)
    filesystem_client = service_client.get_file_system_client(FILESYSTEM)
    return filesystem_client


def upload_file(filesystem_client, local_file):
    destination = f"landing/orders/{local_file.parent.name}/{local_file.name}"

    file_client = filesystem_client.get_file_client(destination)

    with open(local_file, "rb") as data:
        file_client.upload_data(data, overwrite=True)

    print(f"Uploaded -> {destination}")


def main():
    filesystem_client = get_filesystem_client()

    uploaded = 0

    # subimos carpeta por carpeta (batch), con pausa entre cada una
    batch_folders = sorted(LOCAL_FOLDER.glob("batch_*"))

    for batch_folder in batch_folders:
        csv_files = sorted(batch_folder.glob("*.csv"))

        for csv_file in csv_files:
            upload_file(filesystem_client, csv_file)
            uploaded += 1

        print(f"--- Batch {batch_folder.name} subido ({len(csv_files)} archivos) ---")
        time.sleep(SECONDS_BETWEEN_BATCHES)

    print(f"\nTotal uploaded: {uploaded}")


if __name__ == "__main__":
    main()