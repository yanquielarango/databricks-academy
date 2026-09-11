import os

from databricks.ai_search.client import AISearchClient


ENDPOINT_NAME = "data-platform-rag-endpoint"
INDEX_NAME = "dbr_dev.yanquiel_silver.rag_documents_index"


def main():
    client = AISearchClient(
        workspace_url=os.environ["DATABRICKS_HOST"],
        personal_access_token=os.environ["DATABRICKS_TOKEN"],
    )

    index = client.get_index(
        endpoint_name=ENDPOINT_NAME,
        index_name=INDEX_NAME,
    )

    print("\nCurrent index status:")
    print(index.describe())

    print("\nStarting sync...")
    index.sync()

    print("Sync requested successfully.")


if __name__ == "__main__":
    main()