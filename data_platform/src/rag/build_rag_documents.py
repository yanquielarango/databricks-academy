from pathlib import Path

from databricks.connect import DatabricksSession

from rag.chunk_documents import split_markdown


CATALOG = "dbr_dev"
SCHEMA = "yanquiel_silver"
TABLE = "rag_documents"

TARGET_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

KNOWLEDGE_DIR = Path("knowledge")


def build_chunk_rows():
    rows = []

    for path in KNOWLEDGE_DIR.glob("*.md"):
        content = path.read_text(encoding="utf-8").strip()

        if not content:
            continue

        document_id = path.stem
        document_name = path.name
        source_path = str(path)

        chunks = split_markdown(content)

        for chunk_index, chunk_content in enumerate(chunks):
            chunk_id = f"{document_id}_{chunk_index:04d}"

            rows.append(
                (
                    chunk_id,
                    document_id,
                    document_name,
                    source_path,
                    chunk_index,
                    chunk_content,
                )
            )

    return rows


def main():
    spark = (
        DatabricksSession.builder
        .serverless()
        .getOrCreate()
    )

    print("\nBuilding RAG documents...")
    print(f"Target table: {TARGET_TABLE}")

    rows = build_chunk_rows()

    if not rows:
        raise RuntimeError("No Markdown documents were found.")

    chunks_df = spark.createDataFrame(
        rows,
        schema="""
            chunk_id STRING,
            document_id STRING,
            document_name STRING,
            source_path STRING,
            chunk_index INT,
            chunk_content STRING
        """,
    )

    (
        chunks_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )

    print(f"\nRAG documents written to: {TARGET_TABLE}")
    print(f"Number of chunks: {len(rows)}")


if __name__ == "__main__":
    main()