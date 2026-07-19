import os
from pathlib import Path

from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient


load_dotenv()


CONNECTION_STRING = os.getenv(
    "AZURE_STORAGE_CONNECTION_STRING"
)

FILESYSTEM = os.getenv(
    "FILESYSTEM"
)

LOCAL_FOLDER = Path(
    os.getenv(
        "LOCAL_FOLDER",
        "landing_local/orders"
    )
)



def get_filesystem_client():

    service_client = (
        DataLakeServiceClient
        .from_connection_string(
            CONNECTION_STRING
        )
    )

    filesystem_client = (
        service_client
        .get_file_system_client(
            FILESYSTEM
        )
    )

    return filesystem_client



def upload_file(
    filesystem_client,
    local_file
):

    destination = (
        f"landing/orders/"
        f"{local_file.parent.name}/"
        f"{local_file.name}"
    )


    file_client = (
        filesystem_client
        .get_file_client(
            destination
        )
    )


    with open(local_file, "rb") as data:

        file_client.upload_data(
            data,
            overwrite=True
        )


    print(
        f"Uploaded -> {destination}"
    )



def main():

    filesystem_client = (
        get_filesystem_client()
    )

    uploaded = 0


    for csv_file in LOCAL_FOLDER.glob(
        "*/*.csv"
    ):

        upload_file(
            filesystem_client,
            csv_file
        )

        uploaded += 1


    print(
        f"\nTotal uploaded: {uploaded}"
    )



if __name__ == "__main__":
    main()