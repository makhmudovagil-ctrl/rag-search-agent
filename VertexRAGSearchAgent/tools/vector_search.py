"""
Vector Search Tool — semantic expert discovery via Vertex AI + Spanner.

Embeds the query text with Vertex AI `text-embedding-005`
(task_type=RETRIEVAL_QUERY) and ranks experts by cosine distance against
**two** embedding corpora, UNION-ed and deduplicated per expert:

  1. `knowledge_artifact.text_embedding` (473 rows; transcripts, bios, Q&A)
  2. `employment_record.responsibilities_embedding` (5,569 rows; free-form
     job responsibilities)

Both columns were populated by `scripts/migrations/07_generate_embeddings.py`
on 2026-04-11. Including the responsibility side closes the recall gap for
duty-oriented queries ("managed a P&L", "oversaw plant operations",
"navigated SAP transition") where the responsibility corpus is ~12× larger
than the artifact corpus and matches such queries much more tightly.

Path from match row to expert:
  - artifact path:
      knowledge_artifact
        → edge_relevant_employment (M:N, carries context_type)
        → employment_record
        → expert
  - responsibility path:
      employment_record → expert  (direct — responsibilities live on er itself)
  - both paths: LEFT JOIN edge_at_company → company  (for role/company context)

Each result row is tagged with a `match_source` field ("artifact" or
"responsibility") so downstream (Synthesizer) can cite the right evidence.

Dedup-by-expert is done in Python: rows are fetched ordered by distance
ASC, and a simple first-wins pass keeps each expert's single best match
across both sources. We over-fetch (`limit * 10`) to guarantee enough
unique experts after dedup even when one expert has many high-scoring
matches across both corpora.
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
_KG_DB = os.getenv("SPANNER_DATABASE_ID", "kg_products_v5_dev")
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
#   1. Single SQL query with `UNION ALL` over two embedding corpora.
#      Spanner sorts the merged result server-side so we get a global
#      ordering by distance, not a per-source one.
#   2. `match_source` ("artifact" | "responsibility") tags every row so
#      the Synthesizer can cite the right evidence.
#   3. `match_id` holds `artifact_id` on the artifact side and
#      `employment_id` on the responsibility side — same STRING type, so
#      the UNION column types line up.
#   4. `CAST(NULL AS STRING)` is required for the responsibility side
#      because UNION ALL needs exactly-matching column types across
#      branches — the responsibility path has no artifact_type and no
#      context_type, so we surface them as NULL.
#   5. We over-fetch (`LIMIT @fetch_limit`) so that after Python-side
#      dedup-by-expert we still have enough unique experts to fill the
#      caller's `@limit`. Heuristic: `limit * 10` — doubled from the
#      pre-UNION value because one expert can now appear twice (once via
#      artifact, once via responsibility) within the top-K distances.
#   6. Dedup-by-expert is done in Python, not SQL, because this Spanner
#      instance does NOT expose `ROW_NUMBER() OVER (PARTITION BY ...)`
#      (verified 2026-04-11: "501 Unsupported built-in function:
#      ROW_NUMBER"). Python-side first-wins dedup is cheap (a few dozen
#      rows) and easy to reason about.
#   7. `edge_at_company` is LEFT-joined on both branches because a
#      handful of historical employment records lack a company
#      assignment — we'd rather return them with NULL company than drop
#      them silently.
#   8. The artifact branch joins via `edge_relevant_employment` to
#      identify *which* employment an artifact is about. The
#      responsibility branch needs no such edge — responsibilities live
#      on `employment_record` itself, so `er.expert_id` is the direct path.

_VECTOR_SEARCH_SQL = """
SELECT
    e.expert_id,
    e.name,
    'artifact' AS match_source,
    ka.artifact_id AS match_id,
    ka.artifact_type AS match_type,
    ka.text AS match_text,
    er.employment_id,
    er.jobtitle_raw,
    er.is_current,
    er.start_year,
    er.end_year,
    ere.context_type,
    c.name_raw,
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

UNION ALL

SELECT
    e.expert_id,
    e.name,
    'responsibility' AS match_source,
    er.employment_id AS match_id,
    CAST(NULL AS STRING) AS match_type,
    er.responsibilities AS match_text,
    er.employment_id,
    er.jobtitle_raw,
    er.is_current,
    er.start_year,
    er.end_year,
    CAST(NULL AS STRING) AS context_type,
    c.name_raw,
    COSINE_DISTANCE(er.responsibilities_embedding, @q_emb) AS distance
FROM employment_record er
JOIN expert e
    ON er.expert_id = e.expert_id
LEFT JOIN edge_at_company eac
    ON er.employment_id = eac.employment_id
LEFT JOIN company c
    ON eac.company_id = c.company_id
WHERE er.responsibilities_embedding IS NOT NULL

ORDER BY distance ASC
LIMIT @fetch_limit
"""


def _truncate_text(text: Optional[str], max_chars: int = 1500) -> Optional[str]:
    """Trim long match_text so the Synthesizer prompt stays compact.

    Applied to both artifact.text and employment_record.responsibilities;
    responsibilities are usually short, but long-form transcripts can
    blow up the Synthesizer prompt if not capped.
    """
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
    """Semantic expert search via vector similarity over two corpora.

    Embeds `query_text` with Vertex AI `text-embedding-005`
    (task_type=RETRIEVAL_QUERY) and ranks experts by the cosine distance
    between the query embedding and both
    `knowledge_artifact.text_embedding` and
    `employment_record.responsibilities_embedding`. One best-matching
    row per expert is kept, whichever corpus it came from.

    Args:
        query_text: Free-text query for semantic matching.
        limit: Max number of experts to return. Defaults to 20.

    Returns:
        dict with keys:
            - query_type: always "vector"
            - count: number of experts returned
            - results: list of dicts, each containing:
                - expert_id, name
                - match_source: "artifact" | "responsibility"
                - match_id: artifact_id or employment_id (depending on source)
                - match_type: artifact_type (e.g. "Transcript") or None
                - match_text: the matched chunk, truncated to 1500 chars
                - employment_id, jobtitle_raw, is_current, start_year, end_year
                - context_type (artifact branch only)
                - name_raw (may be None)
                - distance: cosine distance (lower = better)
                - similarity: 1 - distance (higher = better)
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

    # Over-fetch multiplier: doubled from 5 (pre-UNION) to 10 because a
    # single expert can now contribute up to two rows (one artifact match
    # + one responsibility match) within the top-K distances.
    params = {
        "q_emb": query_embedding,
        "fetch_limit": int(limit) * 10,
    }
    param_types = {
        "q_emb": spanner.param_types.Array(spanner.param_types.FLOAT32),
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

    # Dedup by expert_id, keeping the first (closest) occurrence across
    # both UNION branches. Rows are already ordered by distance ASC in
    # the SQL, so a simple "first wins" pass gives each expert their
    # single best match — whether it came from an artifact or a
    # responsibility. The match_source field on each kept row tells the
    # Synthesizer which corpus the evidence came from.
    seen_experts: set[str] = set()
    results: list[dict] = []
    for row in rows:
        row_dict = dict(zip(fields, row))
        expert_id = row_dict.get("expert_id")
        if expert_id in seen_experts:
            continue
        seen_experts.add(expert_id)

        # Trim matched text so downstream prompts stay compact.
        row_dict["match_text"] = _truncate_text(row_dict.get("match_text"))
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
