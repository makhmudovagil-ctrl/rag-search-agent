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
    GRAPH_RAW_RESULTS,
    ROUTING_DECISION,
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

    return "\n".join(parts)


def create_synthesizer_agent(model: str = "gemini-2.0-flash") -> LlmAgent:
    """Create the synthesizer LLM agent."""
    return LlmAgent(
        name="synthesizer",
        description="Re-ranks and synthesizes expert search results into a coherent response",
        model=model,
        instruction=_build_instruction,
    )
