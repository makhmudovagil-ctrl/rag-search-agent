"""
Vector Search Tool — stub for MVP.

Returns empty results with a message indicating vector search is not yet implemented.
Will be replaced with BigQuery Vector Search or Vertex AI Vector Search in production.
"""


def search_experts_by_vector(
    query_text: str,
    limit: int = 20,
) -> dict:
    """Semantic search over knowledge artifacts using vector similarity.

    NOTE: This is a stub for MVP — returns empty results.
    Production implementation will use Vertex AI Embedding API (text-embedding-005)
    with BigQuery Vector Search or Spanner ARRAY<FLOAT32> embeddings.

    Args:
        query_text: Free-text query for semantic matching.
        limit: Max results.
    """
    return {
        "query_type": "vector",
        "count": 0,
        "results": [],
        "note": "Vector search not yet implemented. Graph search is the primary retrieval path for MVP.",
    }
