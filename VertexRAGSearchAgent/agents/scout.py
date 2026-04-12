"""
ConditionalScoutAgent — deterministic BaseAgent for conditional chaining.

Reads the routing decision from session state and dispatches the appropriate
search tools. Implements early exit and vector fallback logic:

  "graph"  → graph_search → if count < threshold → vector fallback → merge
  "vector" → vector_search → optional graph enrichment
  "hybrid" → both in parallel → merge
"""

import json
import logging
from typing import AsyncGenerator

from google.adk.agents import BaseAgent
from google.adk.agents.invocation_context import InvocationContext
from google.adk.events import Event, EventActions
from google.genai.types import Content, Part

from VertexRAGSearchAgent.state import (
    COVERAGE_DIAGNOSTICS,
    COVERAGE_ESTIMATE,
    DISAMBIGUATION_RESULT,
    GRAPH_RAW_RESULTS,
    ROUTING_DECISION,
    ROUTING_STRATEGY,
    TEMPORAL_RESULTS,
    VECTOR_RAW_RESULTS,
)
from VertexRAGSearchAgent.tools.graph_search import (
    check_company_disambiguation,
    find_recent_churn,
    get_coverage_diagnostics,
    search_experts_by_company,
    search_experts_by_function,
    search_experts_by_industry,
    search_experts_by_keyword,
    search_experts_by_product,
    search_experts_multi_hop,
)
from VertexRAGSearchAgent.tools.vector_search import search_experts_by_vector

logger = logging.getLogger(__name__)

RESULT_THRESHOLD = 3

# ── Coverage Estimation (P2.4) ──────────────────────────────

_HIGH_THRESHOLD = 0.8
_MODERATE_THRESHOLD = 0.4


def compute_coverage_estimate(actual_count: int, diagnostics: dict) -> dict:
    """Compute coverage fraction from actual results vs estimated pool.

    Pure function — no I/O. Uses pre-computed expert_count / artifact_count
    from the diagnostics dict (produced by get_coverage_diagnostics).

    Args:
        actual_count: Number of unique experts returned by search.
        diagnostics: Dict from get_coverage_diagnostics() with 'product'
            and/or 'company' keys, each having 'matches' with counts.

    Returns:
        Dict with per-entity estimates, e.g.:
        {"company": {"actual": 3, "estimated": 12, "fraction": 0.25, "label": "Low coverage"}}
        Empty dict if no estimates are computable.
    """
    estimate = {}

    company_info = diagnostics.get("company")
    if company_info and company_info.get("status") == "found":
        total = sum(
            m.get("expert_count") or 0 for m in company_info.get("matches", [])
        )
        # Skip when estimated total is 0 or actual already exceeds estimate
        # (estimate is not informative if we found more than the DB thinks exist)
        if total > 0 and actual_count < total:
            fraction = actual_count / total
            estimate["company"] = {
                "actual": actual_count,
                "estimated": total,
                "fraction": fraction,
                "label": _coverage_label(fraction),
            }

    product_info = diagnostics.get("product")
    if product_info and product_info.get("status") == "found":
        total = sum(
            m.get("artifact_count") or 0 for m in product_info.get("matches", [])
        )
        if total > 0 and actual_count < total:
            fraction = actual_count / total
            estimate["product"] = {
                "actual": actual_count,
                "estimated": total,
                "fraction": fraction,
                "label": _coverage_label(fraction),
            }

    return estimate


def _coverage_label(fraction: float) -> str:
    """Return a qualitative label for a coverage fraction."""
    if fraction >= _HIGH_THRESHOLD:
        return "High coverage"
    if fraction >= _MODERATE_THRESHOLD:
        return "Moderate coverage"
    return "Low coverage"


class ConditionalScoutAgent(BaseAgent):
    """Deterministic agent that routes to graph/vector search based on routing decision."""

    name: str = "scout"
    description: str = "Executes graph and/or vector search based on routing decision"

    async def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[Event, None]:
        # Read routing decision from session state
        routing = ctx.session.state.get(ROUTING_DECISION)
        if not routing:
            yield self._make_event(
                ctx, "No routing decision found in state. Cannot proceed.",
                state_delta={GRAPH_RAW_RESULTS: [], VECTOR_RAW_RESULTS: []},
            )
            return

        # Parse routing decision (may be JSON string or dict)
        if isinstance(routing, str):
            routing = json.loads(routing)

        strategy = routing.get("strategy", "graph")
        search_params = routing.get("search_params", [])

        # Build param dict from search_params
        params = {}
        for sp in search_params:
            if isinstance(sp, dict):
                params[sp["param_type"]] = sp["value"]
            else:
                params[sp.param_type] = sp.value

        # Run entity disambiguation when company param is present
        disambiguation = self._run_disambiguation(params)

        # Run temporal search when temporal params are present
        temporal_results = self._run_temporal_search(params)

        if strategy == "graph":
            graph_results = self._run_graph_search(params)
            vector_results = []

            # Early exit check — if sparse, try vector fallback
            if graph_results.get("count", 0) < RESULT_THRESHOLD:
                logger.info(
                    "Graph results below threshold (%d < %d), triggering vector fallback",
                    graph_results.get("count", 0), RESULT_THRESHOLD,
                )
                vector_results = self._run_vector_search(routing, params)

        elif strategy == "vector":
            graph_results = {"query_type": "none", "count": 0, "results": []}
            vector_results = self._run_vector_search(routing, params)

        elif strategy == "hybrid":
            graph_results = self._run_graph_search(params)
            vector_results = self._run_vector_search(routing, params)

        else:
            graph_results = {"query_type": "none", "count": 0, "results": []}
            vector_results = []

        # Merge results and write to state
        graph_list = graph_results.get("results", [])
        vector_list = vector_results.get("results", []) if isinstance(vector_results, dict) else []
        total = len(graph_list) + len(vector_list)

        # Always run coverage diagnostics when entity params exist (P2.4)
        diagnostics = self._run_diagnostics(params)

        # Compute coverage estimate (P2.4)
        coverage_estimate = compute_coverage_estimate(total, diagnostics)

        summary = (
            f"Search complete. Strategy: {strategy}. "
            f"Graph: {len(graph_list)} results. Vector: {len(vector_list)} results. "
            f"Total: {total}."
        )
        if graph_results.get("error"):
            summary += f" Graph error: {graph_results['error']}"
        if diagnostics:
            summary += f" Coverage diagnostics collected for {len(diagnostics)} entities."
        if coverage_estimate:
            for etype, est in coverage_estimate.items():
                summary += f" {etype.title()}: {est['actual']}/{est['estimated']} ({est['label']})."
        if disambiguation.get("status") == "ambiguous":
            summary += f" Disambiguation: company name is ambiguous ({len(disambiguation.get('matches', []))} matches)."
        if temporal_results.get("count", 0) > 0:
            summary += f" Temporal: {temporal_results['count']} recent churn results ({temporal_results.get('churn_type', 'employment')})."

        yield self._make_event(
            ctx,
            summary,
            state_delta={
                ROUTING_STRATEGY: strategy,
                GRAPH_RAW_RESULTS: graph_list,
                VECTOR_RAW_RESULTS: vector_list,
                COVERAGE_DIAGNOSTICS: diagnostics,
                COVERAGE_ESTIMATE: coverage_estimate,
                DISAMBIGUATION_RESULT: disambiguation,
                TEMPORAL_RESULTS: temporal_results,
            },
        )

    def _run_diagnostics(self, params: dict) -> dict:
        """Run coverage diagnostics for sparse results.

        Args:
            params: Search parameters extracted from routing decision.

        Returns:
            Diagnostics dict from get_coverage_diagnostics(), or empty dict on
            failure or when no diagnosable entities are present.
        """
        product_name = params.get("product")
        company_name = params.get("company")

        if not product_name and not company_name:
            return {}

        try:
            return get_coverage_diagnostics(
                product_name=product_name,
                company_name=company_name,
            )
        except Exception as e:
            logger.error("Coverage diagnostics failed: %s", e)
            return {}

    def _run_disambiguation(self, params: dict) -> dict:
        """Run entity disambiguation for company parameter.

        Args:
            params: Search parameters extracted from routing decision.

        Returns:
            Disambiguation dict from check_company_disambiguation(), or empty
            dict when no company param or on failure.
        """
        company_name = params.get("company")
        if not company_name:
            return {}

        try:
            return check_company_disambiguation(company_name)
        except Exception as e:
            logger.error("Disambiguation failed: %s", e)
            return {}

    def _run_temporal_search(self, params: dict) -> dict:
        """Run temporal churn search when temporal params are present.

        Args:
            params: Search parameters extracted from routing decision.

        Returns:
            Temporal results dict from find_recent_churn(), or empty dict
            when no temporal params or on failure.
        """
        temporal_months = params.get("temporal_months")
        if not temporal_months:
            return {}

        entity_name = params.get("company") or params.get("product")
        if not entity_name:
            return {}

        try:
            duration = int(temporal_months)
        except (ValueError, TypeError):
            duration = 12

        churn_type = params.get("churn_type", "employment")

        try:
            return find_recent_churn(
                entity_name=entity_name,
                duration_months=duration,
                churn_type=churn_type,
            )
        except Exception as e:
            logger.error("Temporal search failed: %s", e)
            return {}

    def _run_graph_search(self, params: dict) -> dict:
        """Dispatch to the appropriate graph search function based on params."""
        has_product = "product" in params
        has_company = "company" in params
        has_industry = "industry" in params
        has_function = "function" in params
        has_keyword = "keyword" in params

        is_current = None
        if "is_current_role" in params:
            is_current = params["is_current_role"].lower() == "true"

        # Multi-constraint → multi_hop search
        constraint_count = sum([has_product, has_company, has_industry, has_function])
        if constraint_count >= 2:
            return search_experts_multi_hop(
                product_name=params.get("product"),
                company_name=params.get("company"),
                industry_name=params.get("industry"),
                function=params.get("function"),
                supply_chain_position=params.get("supply_chain_position"),
                is_current_role=is_current,
            )

        # Single-constraint searches
        if has_product:
            return search_experts_by_product(
                product_name=params["product"],
                supply_chain_position=params.get("supply_chain_position"),
                is_current_role=is_current,
            )
        if has_company:
            return search_experts_by_company(
                company_name=params["company"],
                function=params.get("function"),
                is_current_role=is_current,
            )
        if has_industry:
            return search_experts_by_industry(
                industry_name=params["industry"],
                function=params.get("function"),
                is_current_role=is_current,
            )
        if has_function:
            return search_experts_by_function(
                function=params["function"],
                seniority=params.get("seniority"),
                industry_name=params.get("industry"),
                is_current_role=is_current,
            )
        if has_keyword:
            return search_experts_by_keyword(keyword=params["keyword"])

        return {"query_type": "none", "count": 0, "results": [],
                "error": "No searchable parameters extracted from query"}

    def _run_vector_search(self, routing: dict, params: dict) -> dict:
        """Run vector search (stub for MVP)."""
        query_text = routing.get("reasoning", "")
        keywords = [sp.get("value", "") if isinstance(sp, dict) else sp.value
                    for sp in routing.get("search_params", [])
                    if (isinstance(sp, dict) and sp.get("param_type") == "keyword")
                    or (hasattr(sp, "param_type") and sp.param_type == "keyword")]
        search_text = " ".join(keywords) if keywords else query_text
        return search_experts_by_vector(search_text)

    def _make_event(self, ctx: InvocationContext, text: str,
                    state_delta: dict = None) -> Event:
        return Event(
            invocation_id=ctx.invocation_id,
            author=self.name,
            branch=ctx.branch,
            # No content — scout output is internal, only state is passed forward
            actions=EventActions(state_delta=state_delta or {}),
        )
