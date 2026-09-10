import re


MAX_CHUNK_SIZE = 1500
CHUNK_OVERLAP = 200


def split_large_section(
    text: str,
    max_chunk_size: int = MAX_CHUNK_SIZE,
    overlap: int = CHUNK_OVERLAP,
) -> list[str]:
    if len(text) <= max_chunk_size:
        return [text]

    chunks = []
    start = 0

    while start < len(text):
        end = min(start + max_chunk_size, len(text))

        chunk = text[start:end].strip()

        if chunk:
            chunks.append(chunk)

        if end == len(text):
            break

        start = end - overlap

    return chunks


def split_markdown(text: str) -> list[str]:
    if not text:
        return []

    sections = re.split(
        r"(?=^#{1,2}\s+)",
        text,
        flags=re.MULTILINE,
    )

    chunks = []

    for section in sections:
        section = section.strip()

        if not section:
            continue

        chunks.extend(split_large_section(section))

    return chunks