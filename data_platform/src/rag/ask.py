import os
import sys

from databricks.ai_search.client import AISearchClient
from openai import OpenAI

from rag.retriever import search_documents


LLM_ENDPOINT = "databricks-qwen3-next-80b-a3b-instruct"


def build_context(search_results) -> str:
    rows = search_results.get("result", {}).get("data_array", [])

    context_parts = []

    for row in rows:
        document_name = row[2]
        chunk_content = row[5]

        context_parts.append(
            f"Source: {document_name}\n\n{chunk_content}"
        )

    return "\n\n---\n\n".join(context_parts)


def extract_sources(search_results) -> list[str]:
    rows = search_results.get("result", {}).get("data_array", [])

    sources = []

    for row in rows:
        document_name = row[2]

        if document_name not in sources:
            sources.append(document_name)

    return sources


def ask_llm(
    question: str,
    context: str,
) -> str:
    workspace_url = os.environ["DATABRICKS_HOST"]
    token = os.environ["DATABRICKS_TOKEN"]

    client = OpenAI(
        api_key=token,
        base_url=f"{workspace_url}/serving-endpoints",
    )

    response = client.chat.completions.create(
        model=LLM_ENDPOINT,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are an assistant for the data platform project. "
                    "Answer only using the provided context. "
                    "Do not invent information. "
                    "If the answer is not contained in the context, say that "
                    "the available project documentation does not contain "
                    "enough information to answer the question."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Context:\n\n{context}\n\n"
                    f"Question:\n{question}"
                ),
            },
        ],
        temperature=0.1,
        max_tokens=500,
    )

    return response.choices[0].message.content


def main():
    if len(sys.argv) < 2:
        raise SystemExit(
            'Usage: uv run python src/rag/ask.py "your question"'
        )

    question = " ".join(sys.argv[1:])

    workspace_url = os.environ["DATABRICKS_HOST"]
    token = os.environ["DATABRICKS_TOKEN"]

    search_client = AISearchClient(
        workspace_url=workspace_url,
        personal_access_token=token,
    )

    search_results = search_documents(
        client=search_client,
        query=question,
        num_results=5,
    )

    context = build_context(search_results)
    sources = extract_sources(search_results)

    answer = ask_llm(
        question=question,
        context=context,
    )

    print("\nQUESTION")
    print(question)

    print("\nANSWER")
    print(answer)

    print("\nSOURCES")

    for source in sources:
        print(f"- {source}")


if __name__ == "__main__":
    main()