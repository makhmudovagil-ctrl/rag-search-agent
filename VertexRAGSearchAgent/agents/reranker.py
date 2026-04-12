"""
Re-ranker — Gemini-based contextual re-ranking of search results.

Merges graph + vector results, de-duplicates by expert name, and uses
a structured Gemini call to score and rank experts by relevance to the
original query. Runs between Scout and Synthesizer (Step 2.5).
"""

import json
import logging
from typing import Optional

from google.genai import Client
from google.genai.types import Content, GenerateContentConfig, Part
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

_MAX_EXPERTS_INPUT = 30
_MAX_EXPERTS_OUTPUT = 20


# ── Pydantic schemas ─────────────────────────────────────────────────────────

class RankedExpert(BaseModel):
    """A single expert with relevance score and ranking justification."""

    expert_name: str = Field(description="Full name of the expert")
    relevance_score: float = Field(
        description="Relevance score from 0.0 (irrelevant) to 1.0 (perfect match)",
        ge=0.0, le=1.0,
    )
    rank: int = Field(description="Rank position (1 = most relevant)")
    ranking_reasoning: str = Field(
        description="One-line justification for this ranking position"
    )
    source: str = Field(
        description='Which search found this expert: "graph", "vector", or "both"'
    )


class RerankedResults(BaseModel):
    """Structured output from the re-ranking Gemini call."""

    experts: list[RankedExpert] = Field(
        description="Experts ordered by descending relevance score"
    )


# ─�� Re-ranker instruction ────────────────────────────────────────────────────

RERANKER_INSTRUCTION = """You are a relevance ranking engine for an expert discovery pipeline.

You will receive a user query and a list of expert candidates found by search.
Your job is to score each expert (0.0–1.0) and return them in ranked order.

## Ranking Criteria (in priority order)

1. **Query Relevance (primary):** How well does the expert's role, company, products,
   and industry match the user's query intent? Exact matches score higher than partial.

2. **Recency (secondary):** Current roles are preferred over past roles.
   An expert currently at the target company/product scores higher than a former one.

3. **Seniority (tertiary):** For leadership or strategic queries, higher seniority
   scores higher. For technical queries, hands-on practitioners may score higher.

4. **Supply Chain Match (tertiary):** When the query specifies a supply chain position
   (buyer, seller, vendor), exact position matches score higher.

5. **Vector Similarity (input signal):** If an expert has a `similarity_score` from
   vector search, use it as an additional signal — higher similarity = higher relevance.

## Scoring Guide

- 0.9–1.0: Perfect match — current role, exact company/product, right function
- 0.7–0.89: Strong match — right company but past role, or current role but adjacent function
- 0.5–0.69: Moderate match — related company/product, relevant experience
- 0.3–0.49: Weak match — tangential connection, different department
- 0.0–0.29: Poor match — minimal relevance to query

## Rules

- Return at most 20 experts.
- Preserve ALL original data fields for each expert — do not omit any.
- Set `source` to "graph", "vector", or "both" based on the `_source` tag.
- Provide a concise one-line `ranking_reasoning` for each expert.
- Order the output list by descending `relevance_score`.
- Number ranks starting from 1.
"""


# ── Merge and de-duplication ─────────────────────────────────────────────────

def merge_and_dedup(
    graph_results: list[dict],
    vector_results: list[dict],
) -> list[dict]:
    """Merge graph and vector results, de-duplicate by expert name.

    Tags each result with '_source' ("graph" or "vector"). When the same
    expert appears in both, keeps the entry with more populated fields and
    marks source as "both".

    Args:
        graph_results: Expert dicts from graph search.
        vector_results: Expert dicts from vector search.

    Returns:
        Merged, de-duplicated list capped at _MAX_EXPERTS_INPUT.
    """
    seen: dict[str, dict] = {}

    for result in graph_results:
        result_copy = {**result, "_source": "graph"}
        name = _normalize_name(result_copy.get("expert_name", ""))
        if not name:
            continue
        seen[name] = result_copy

    for result in vector_results:
        result_copy = {**result, "_source": "vector"}
        name = _normalize_name(result_copy.get("expert_name", ""))
        if not name:
            continue

        if name in seen:
            # Keep the entry with more populated fields, mark as "both"
            existing = seen[name]
            if _field_count(result_copy) > _field_count(existing):
                result_copy["_source"] = "both"
                seen[name] = result_copy
            else:
                existing["_source"] = "both"
        else:
            seen[name] = result_copy

    merged = list(seen.values())
    return merged[:_MAX_EXPERTS_INPUT]


def _normalize_name(name: str) -> str:
    """Normalize expert name for dedup comparison."""
    return name.strip().lower()


def _field_count(d: dict) -> int:
    """Count non-None, non-empty values in a dict."""
    return sum(1 for v in d.values() if v is not None and v != "" and v != [])


# ── Gemini re-ranking call ───────────────────────────────────────────────────

def run_reranker(
    client: Client,
    model: str,
    user_query: str,
    merged_results: list[dict],
) -> list[dict]:
    """Run Gemini re-ranking on merged search results.

    Args:
        client: Google GenAI client instance.
        model: Model name (e.g. "gemini-2.0-flash").
        user_query: Original user query text.
        merged_results: Merged and de-duplicated expert list.

    Returns:
        List of ranked expert dicts with relevance_score, rank, and
        ranking_reasoning. Empty list on any error (pipeline continues
        with unranked results).
    """
    if not merged_results:
        return []

    try:
        # Build user content with query and results
        user_content = (
            f"User query: {user_query}\n\n"
            f"Experts to rank ({len(merged_results)} candidates):\n"
            f"{json.dumps(merged_results, default=str, indent=2)}"
        )

        response = client.models.generate_content(
            model=model,
            contents=[Content(parts=[Part(text=user_content)], role="user")],
            config=GenerateContentConfig(
                system_instruction=RERANKER_INSTRUCTION,
                response_mime_type="application/json",
                response_schema=RerankedResults,
            ),
        )

        parsed = json.loads(response.text)
        experts = parsed.get("experts", [])

        # Enrich each ranked expert with original data
        originals_by_name = {
            _normalize_name(r.get("expert_name", "")): r
            for r in merged_results
        }

        ranked = []
        for expert in experts[:_MAX_EXPERTS_OUTPUT]:
            name = _normalize_name(expert.get("expert_name", ""))
            original = originals_by_name.get(name, {})
            ranked.append({
                "expert_name": expert.get("expert_name", "unknown"),
                "relevance_score": expert.get("relevance_score", 0.0),
                "rank": expert.get("rank", 0),
                "ranking_reasoning": expert.get("ranking_reasoning", ""),
                "source": expert.get("source", original.get("_source", "unknown")),
                "data": {k: v for k, v in original.items() if k != "_source"},
            })

        logger.info(
            "Re-ranked %d experts from %d candidates",
            len(ranked), len(merged_results),
        )
        return ranked

    except Exception as e:
        logger.error("Re-ranker failed, falling back to unranked results: %s", e)
        return []
