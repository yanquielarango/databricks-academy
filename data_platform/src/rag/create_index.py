import os

from databricks.ai_search.client import AISearchClient


ENDPOINT_NAME = "data-platform-rag-endpoint"
SOURCE_TABLE = "dbr_dev.yanquiel_silver.rag_documents"
INDEX_NAME = "dbr_dev.yanquiel_silver.rag_documents_index"
EMBEDDING_MODEL_ENDPOINT = "databricks-qwen3-embedding-0-6b"


def main():
    client = AISearchClient(
        workspace_url=os.environ["DATABRICKS_HOST"],
        personal_access_token=os.environ["DATABRICKS_TOKEN"],
    )

    print("\nCreating AI Search index...")
    print(f"Endpoint:     {ENDPOINT_NAME}")
    print(f"Source table: {SOURCE_TABLE}")
    print(f"Index:        {INDEX_NAME}")
    print(f"Embedding:    {EMBEDDING_MODEL_ENDPOINT}")

    index = client.create_delta_sync_index(
        endpoint_name=ENDPOINT_NAME,
        source_table_name=SOURCE_TABLE,
        index_name=INDEX_NAME,
        pipeline_type="TRIGGERED",
        primary_key="chunk_id",
        embedding_source_column="chunk_content",
        embedding_model_endpoint_name=EMBEDDING_MODEL_ENDPOINT,
    )

    print("\nIndex creation requested.")
    print(index.describe())


if __name__ == "__main__":
    main()