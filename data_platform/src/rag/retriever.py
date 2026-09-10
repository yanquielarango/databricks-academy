from databricks.ai_search.client import AISearchClient


INDEX_NAME = "dbr_dev.yanquiel_silver.rag_documents_index"

DEFAULT_COLUMNS = [
    "chunk_id",
    "document_id",
    "document_name",
    "source_path",
    "chunk_index",
    "chunk_content",
]


def search_documents(
    client: AISearchClient,
    query: str,
    num_results: int = 5,
    columns: list[str] | None = None,
):
    index = client.get_index(
        index_name=INDEX_NAME,
    )

    results = index.similarity_search(
        query_text=query,
        columns=columns or DEFAULT_COLUMNS,
        num_results=num_results,
    )

    return results