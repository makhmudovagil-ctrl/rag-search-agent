"""
Vector Search Tool — semantic expert discovery via Vertex AI + Spanner.

Embeds the query text with Vertex AI `text-embedding-005`
(task_type=RETRIEVAL_QUERY) and ranks experts by
`COSINE_DISTANCE(knowledge_artifact.text_embedding, @query_embedding)`.

The knowledge_artifact.text_embedding column was populated by
`scripts/migrations/07_generate_embeddings.py` on 2026-04-11 for all
473 eligible rows.

Path from artifact to expert:
    knowledge_artifact
        → edge_relevant_employment (M:N, carries context_type)
        → employment_record
        → expert
    + LEFT JOIN edge_at_company → company   (for jobtitle/company context)

The SQL collapses multiple artifacts per expert using
`ROW_NUMBER() OVER (PARTITION BY expert_id ORDER BY distance ASC)` and
keeps only each expert's best-matching artifact, so the caller sees one
row per expert ranked by semantic similarity.
"""

import logging
import os
from typing import Optional

import google.auth
import vertexai
from google.cloud import spanner
from google.cloud.spanner_v1.database import Database as SpannerDatabase
from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel

logger = logging.getLogger(__name__)

# ── Connection config ──────────────────────────────────────────────────────────
_PROJECT = os.getenv("GCP_PROJECT_ID", "gcp-poc-488614")
_REGION = os.getenv("GCP_REGION", "us-central1")
_INSTANCE = os.getenv("SPANNER_INSTANCE_ID", "kg-dev-instance")
_KG_DB = os.getenv("SPANNER_DATABASE_ID", "kg_products_v4_dev")
_DEFAULT_LIMIT = 20

# Embedding model — must match what 07_generate_embeddings.py wrote.
_EMBEDDING_MODEL_NAME = "text-embedding-005"
_EMBEDDING_DIMS = 768
_TASK_TYPE_QUERY = "RETRIEVAL_QUERY"

# Lazy singletons (one per process).
_client: Optional[spanner.Client] = None
_kg_db: Optional[SpannerDatabase] = None
_vertex_initialized: bool = False
_embed_model: Optional[TextEmbeddingModel] = None


# ── Spanner / Vertex lazy init ─────────────────────────────────────────────────

def _get_kg_db() -> SpannerDatabase:
    """Return a cached Spanner Database handle using ADC."""
    global _client, _kg_db
    if _kg_db is None:
        creds, _ = google.auth.default()
        _client = spanner.Client(project=_PROJECT, credentials=creds)
        _kg_db = _client.instance(_INSTANCE).database(_KG_DB)
    return _kg_db


def _get_embed_model() -> TextEmbeddingModel:
    """Return a cached Vertex AI TextEmbeddingModel instance."""
    global _vertex_initialized, _embed_model
    if not _vertex_initialized:
        vertexai.init(project=_PROJECT, location=_REGION)
        _vertex_initialized = True
    if _embed_model is None:
        _embed_model = TextEmbeddingModel.from_pretrained(_EMBEDDING_MODEL_NAME)
    return _embed_model


def _embed_query(query_text: str) -> list[float]:
    """Embed a single query string with task_type=RETRIEVAL_QUERY.

    Returns a list of 768 floats. Raises on failure — callers handle.
    """
    model = _get_embed_model()
    result = model.get_embeddings(
        [TextEmbeddingInput(text=query_text, task_type=_TASK_TYPE_QUERY)],
        auto_truncate=True,
    )
    if not result or len(result) != 1:
        raise RuntimeError(
            f"Vertex AI embedding returned {len(result) if result else 0} vectors, expected 1"
        )
    values = list(result[0].values)
    if len(values) != _EMBEDDING_DIMS:
        raise RuntimeError(
            f"Vertex AI embedding returned {len(values)} dims, expected {_EMBEDDING_DIMS}"
        )
    return values


# ── Ranking SQL ────────────────────────────────────────────────────────────────
#
# Design notes:
#   1. We filter out artifacts without an embedding so the distance
#      computation only runs on populated rows.
#   2. We over-fetch (`LIMIT @fetch_limit`) so that after deduping by
#      expert in Python we still have enough unique experts to fill the
#      caller's `@limit`. A simple heuristic of `limit * 5` is plenty.
#   3. Dedup-by-expert is done in Python instead of SQL because the
#      Spanner GoogleSQL surface on this instance does **not** expose
#      `ROW_NUMBER() OVER (PARTITION BY ...)` as a built-in (verified
#      2026-04-11: "501 Unsupported built-in function: ROW_NUMBER").
#      Since we only pull a few dozen rows, Python-side dedup is cheap
#      and easy to reason about.
#   4. `edge_at_company` is LEFT-joined because a handful of historical
#      employment records lack a company assignment — we'd rather return
#      them with NULL company than drop them silently.
#   5. `artifact_type` is useful context for the Synthesizer ("call
#      transcript" vs. "bio" vs. "qa") — included in the projection.

_VECTOR_SEARCH_SQL = """
SELECT
    e.expert_id,
    e.expert_name,
    ka.artifact_id,
    ka.artifact_type,
    ka.text AS artifact_text,
    er.employment_id,
    er.jobtitle_raw,
    er.is_current,
    er.start_year,
    er.end_year,
    ere.context_type,
    c.company_name,
    COSINE_DISTANCE(ka.text_embedding, @q_emb) AS distance
FROM knowledge_artifact ka
JOIN edge_relevant_employment ere
    ON ka.artifact_id = ere.artifact_id
JOIN employment_record er
    ON ere.employment_id = er.employment_id
JOIN expert e
    ON er.expert_id = e.expert_id
LEFT JOIN edge_at_company eac
    ON er.employment_id = eac.employment_id
LEFT JOIN company c
    ON eac.company_id = c.company_id
WHERE ka.text_embedding IS NOT NULL
ORDER BY distance ASC
LIMIT @fetch_limit
"""


def _truncate_text(text: Optional[str], max_chars: int = 1500) -> Optional[str]:
    """Trim long artifact_text so the Synthesizer prompt stays compact."""
    if text is None:
        return None
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "…"


# ── Public tool function ───────────────────────────────────────────────────────

def search_experts_by_vector(
    query_text: str,
    limit: int = _DEFAULT_LIMIT,
) -> dict:
    """Semantic expert search over knowledge artifacts via vector similarity.

    Embeds `query_text` with Vertex AI `text-embedding-005`
    (task_type=RETRIEVAL_QUERY) and ranks experts by the cosine distance
    between the query embedding and each artifact's `text_embedding`.
    One best-matching artifact is kept per expert.

    Args:
        query_text: Free-text query for semantic matching.
        limit: Max number of experts to return. Defaults to 20.

    Returns:
        dict with keys:
            - query_type: always "vector"
            - count: number of rows returned
            - results: list of dicts (expert + best artifact + company)
            - error: present only when the call failed
    """
    if not query_text or not query_text.strip():
        return {
            "query_type": "vector",
            "count": 0,
            "results": [],
            "error": "query_text is empty",
        }

    try:
        query_embedding = _embed_query(query_text)
    except Exception as e:
        logger.error("vector_search: embedding failed: %s", e)
        return {
            "query_type": "vector",
            "count": 0,
            "results": [],
            "error": f"embedding_failed: {type(e).__name__}: {e}",
        }

    params = {
        "q_emb": query_embedding,
        "limit": int(limit),
        "fetch_limit": int(limit) * 5,
    }
    param_types = {
        "q_emb": spanner.param_types.Array(spanner.param_types.FLOAT32),
        "limit": spanner.param_types.INT64,
        "fetch_limit": spanner.param_types.INT64,
    }

    try:
        db = _get_kg_db()
        with db.snapshot() as snap:
            result = snap.execute_sql(
                _VECTOR_SEARCH_SQL,
                params=params,
                param_types=param_types,
            )
            rows = list(result)
            fields = [f.name for f in result.fields]
    except Exception as e:
        logger.error("vector_search: spanner query failed: %s", e)
        return {
            "query_type": "vector",
            "count": 0,
            "results": [],
            "error": f"spanner_failed: {type(e).__name__}: {e}",
        }

    # Dedup by expert_id, keeping the first (closest) occurrence. Rows are
    # already ordered by distance ASC in the SQL, so a simple "first wins"
    # pass gives each expert their best-matching artifact.
    seen_experts: set[str] = set()
    results: list[dict] = []
    for row in rows:
        row_dict = dict(zip(fields, row))
        expert_id = row_dict.get("expert_id")
        if expert_id in seen_experts:
            continue
        seen_experts.add(expert_id)

        # Trim text so downstream prompts stay compact.
        row_dict["artifact_text"] = _truncate_text(row_dict.get("artifact_text"))
        # Distance → similarity score (1 - cosine distance), rounded for display.
        distance = row_dict.get("distance")
        if distance is not None:
            row_dict["similarity"] = round(1.0 - float(distance), 4)
            row_dict["distance"] = round(float(distance), 4)
        results.append(row_dict)

        if len(results) >= int(limit):
            break

    return {
        "query_type": "vector",
        "count": len(results),
        "results": results,
    }
