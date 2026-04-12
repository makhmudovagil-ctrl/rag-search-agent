"""
Synthesizer Agent — re-ranks and synthesizes expert search results.

Reads graph/vector results from session state (injected into instruction)
and produces a coherent, ranked response for the user.
"""

import json

from google.adk.agents import LlmAgent
from google.adk.agents.readonly_context import ReadonlyContext
from VertexRAGSearchAgent.state import (
    COVERAGE_DIAGNOSTICS,
    COVERAGE_ESTIMATE,
    DISAMBIGUATION_RESULT,
    GRAPH_RAW_RESULTS,
    ROUTING_DECISION,
    TEMPORAL_RESULTS,
    VECTOR_RAW_RESULTS,
)

_BASE_INSTRUCTION = """You are the synthesis component of an expert discovery pipeline.
Your job is to take raw search results and produce a clear, ranked list of expert candidates.

## Your Tasks

1. **De-duplicate**: Same expert may appear multiple times (different employment records). Group by expert.
2. **Rank**: Order by relevance to the original query:
   - Current role > past role
   - Direct product involvement > tangential involvement
   - Higher seniority > lower for leadership queries
   - Supply chain position match > general involvement
3. **Synthesize**: For each expert, provide:
   - Name and current/most relevant role
   - Company and industry context
   - Why they match the query (specific evidence from the data)
   - Supply chain position if relevant (buyer, seller, etc.)
4. **Coverage note**: State how many unique experts were found and from which sources.
   If results are sparse (< 3), suggest how to broaden the search.

## Output Format

Present results as a numbered list. Each entry should be concise but informative.
End with a brief coverage summary.

CRITICAL: Do NOT fabricate information not present in the search results below.
If no results were found, say so clearly and suggest alternative search strategies.
"""


def _format_diagnostics(diagnostics: dict) -> str:
    """Format coverage diagnostics dict into human-readable text for the LLM.

    Args:
        diagnostics: Dict from get_coverage_diagnostics() with 'product'
            and/or 'company' keys.

    Returns:
        Formatted multi-line string describing each entity's status.
    """
    lines = []
    for entity_type in ("product", "company"):
        info = diagnostics.get(entity_type)
        if not info:
            continue

        name = info.get("name", "unknown")
        status = info.get("status", "unknown")

        if status == "not_found":
            lines.append(f"- **{entity_type.title()} '{name}'**: Not found in the database.")
        elif status == "found":
            matches = info.get("matches", [])
            for match in matches:
                match_name = match.get("name", name)
                expert_count = match.get("expert_count")
                artifact_count = match.get("artifact_count")
                count_label = (
                    f"{expert_count} linked experts"
                    if expert_count is not None
                    else f"{artifact_count} knowledge artifacts"
                    if artifact_count is not None
                    else "unknown coverage"
                )
                lines.append(f"- **{entity_type.title()} '{match_name}'**: Found — {count_label}.")

    return "\n".join(lines) if lines else "No diagnosable entities."


def _format_disambiguation(disambiguation: dict) -> str:
    """Format disambiguation data into human-readable text for the LLM.

    Args:
        disambiguation: Dict from check_company_disambiguation() with
            'status', 'matches', and 'aliases' keys.

    Returns:
        Formatted multi-line string describing the ambiguity.
    """
    name = disambiguation.get("name", "unknown")
    matches = disambiguation.get("matches", [])
    aliases = disambiguation.get("aliases", [])

    lines = [f"The company name **'{name}'** is ambiguous. Multiple entities match:"]

    for match in matches:
        company_name = match.get("company_name", "unknown")
        expert_count = match.get("expert_count", 0)
        company_id = match.get("company_id", "")
        flag = " (flagged)" if match.get("ambiguity_flag") else ""
        lines.append(f"- **{company_name}** — {expert_count} linked experts{flag}")

        # List aliases for this specific company
        company_aliases = [a for a in aliases if a.get("company_id") == company_id]
        for alias in company_aliases:
            lines.append(f"  - Also known as: {alias['alias_name']} ({alias.get('alias_type', 'alias')})")

    return "\n".join(lines)


def _format_temporal(temporal: dict) -> str:
    """Format temporal search results into human-readable text for the LLM.

    Args:
        temporal: Dict from find_recent_churn() with 'churn_type',
            'entity_name', 'duration_months', 'cutoff', and 'results' keys.

    Returns:
        Formatted multi-line string describing the temporal results.
    """
    churn_type = temporal.get("churn_type", "employment")
    entity = temporal.get("entity_name", "unknown")
    duration = temporal.get("duration_months", 12)
    results = temporal.get("results", [])
    cutoff = temporal.get("cutoff", "unknown")

    lines = [
        f"Temporal search for **'{entity}'** — {churn_type} churn "
        f"within the last {duration} months (since {cutoff}):",
        f"Found {len(results)} results.",
    ]

    for r in results[:10]:
        if churn_type == "employment":
            name = r.get("expert_name", "unknown")
            title = r.get("jobtitle_raw", "")
            end_y = r.get("end_year", "?")
            end_m = r.get("end_month")
            date_str = f"{end_y}-{end_m:02d}" if isinstance(end_m, int) else str(end_y)
            lines.append(f"- **{name}** — {title}, left ~{date_str}")
        elif churn_type == "involvement":
            name = r.get("expert_name", "unknown")
            product = r.get("product_name", "")
            end_date = r.get("end_date", "?")
            lines.append(f"- **{name}** — stopped using {product}, ended {end_date}")
        elif churn_type == "relationship":
            from_co = r.get("from_company", "unknown")
            to_co = r.get("to_company", "unknown")
            end_date = r.get("end_date", "?")
            rel = r.get("relation_type", "customer")
            lines.append(f"- **{from_co}** → {to_co} ({rel}), ended {end_date}")

    return "\n".join(lines)


def _format_coverage_estimate(estimate: dict) -> str:
    """Format coverage estimate dict into concise per-entity lines.

    Args:
        estimate: Dict with 'company' and/or 'product' sub-dicts,
            each containing 'actual', 'estimated', 'fraction', 'label'.

    Returns:
        Formatted multi-line string, one line per entity.
    """
    lines = []
    for entity_type, info in estimate.items():
        actual = info.get("actual", 0)
        estimated = info.get("estimated", 0)
        label = info.get("label", "Unknown")
        unit = "experts" if entity_type == "company" else "artifacts"
        lines.append(
            f"- {entity_type.title()}: Found {actual} of ~{estimated} "
            f"estimated {unit} ({label})."
        )
    return "\n".join(lines)


def _build_instruction(ctx: ReadonlyContext) -> str:
    """Build synthesizer instruction with actual search results from state."""
    graph_results = ctx.state.get(GRAPH_RAW_RESULTS, [])
    vector_results = ctx.state.get(VECTOR_RAW_RESULTS, [])
    routing = ctx.state.get(ROUTING_DECISION, {})

    parts = [_BASE_INSTRUCTION]

    parts.append(f"\n## Routing Decision\n```json\n{json.dumps(routing, default=str, indent=2)}\n```")

    if graph_results:
        parts.append(f"\n## Graph Search Results ({len(graph_results)} experts)\n```json\n{json.dumps(graph_results, default=str, indent=2)}\n```")
    else:
        parts.append("\n## Graph Search Results\nNo results found.")

    if vector_results:
        parts.append(f"\n## Vector Search Results ({len(vector_results)} experts)\n```json\n{json.dumps(vector_results, default=str, indent=2)}\n```")
    else:
        parts.append("\n## Vector Search Results\nNo results found.")

    # Coverage diagnostics (P1.3) — only present when results are sparse
    diagnostics = ctx.state.get(COVERAGE_DIAGNOSTICS, {})
    if diagnostics:
        parts.append("\n## Coverage Diagnostics")
        parts.append(_format_diagnostics(diagnostics))
        parts.append(
            "Use the diagnostics above to explain to the user why results are "
            "sparse and suggest alternative search strategies."
        )

    # Coverage estimate (P2.4) — present when entity counts are available
    coverage_estimate = ctx.state.get(COVERAGE_ESTIMATE, {})
    if coverage_estimate:
        parts.append("\n## Coverage Estimate")
        parts.append(_format_coverage_estimate(coverage_estimate))
        parts.append(
            "Use the coverage estimate to inform the user how comprehensive "
            "the results are. If coverage is low, suggest broadening the search."
        )

    # Entity disambiguation (P1.4) — only present when company name is ambiguous
    disambiguation = ctx.state.get(DISAMBIGUATION_RESULT, {})
    if disambiguation.get("status") == "ambiguous":
        parts.append("\n## Disambiguation Notice")
        parts.append(_format_disambiguation(disambiguation))
        parts.append(
            "Inform the user about the ambiguity and which entity/entities "
            "the search results correspond to. Suggest refining the query "
            "with a more specific company name if needed."
        )

    # Temporal search results (P2.3) — only present when temporal params detected
    temporal = ctx.state.get(TEMPORAL_RESULTS, {})
    if temporal.get("count", 0) > 0:
        parts.append("\n## Temporal Search Results")
        parts.append(_format_temporal(temporal))
        parts.append(
            "Indicate when each expert left or when the relationship ended. "
            "Highlight how recently the departure occurred relative to the "
            "query timeframe."
        )

    return "\n".join(parts)


def create_synthesizer_agent(model: str = "gemini-2.0-flash") -> LlmAgent:
    """Create the synthesizer LLM agent."""
    return LlmAgent(
        name="synthesizer",
        description="Re-ranks and synthesizes expert search results into a coherent response",
        model=model,
        instruction=_build_instruction,
    )
