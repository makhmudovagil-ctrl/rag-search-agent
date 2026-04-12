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
    DISAMBIGUATION_RESULT,
    GRAPH_RAW_RESULTS,
    ROUTING_DECISION,
    ROUTING_STRATEGY,
    VECTOR_RAW_RESULTS,
)
from VertexRAGSearchAgent.tools.graph_search import (
    check_company_disambiguation,
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

        # Run coverage diagnostics when results are sparse
        diagnostics = {}
        if total < RESULT_THRESHOLD:
            diagnostics = self._run_diagnostics(params)

        summary = (
            f"Search complete. Strategy: {strategy}. "
            f"Graph: {len(graph_list)} results. Vector: {len(vector_list)} results. "
            f"Total: {total}."
        )
        if graph_results.get("error"):
            summary += f" Graph error: {graph_results['error']}"
        if diagnostics:
            summary += f" Coverage diagnostics collected for {len(diagnostics)} entities."
        if disambiguation.get("status") == "ambiguous":
            summary += f" Disambiguation: company name is ambiguous ({len(disambiguation.get('matches', []))} matches)."

        yield self._make_event(
            ctx,
            summary,
            state_delta={
                ROUTING_STRATEGY: strategy,
                GRAPH_RAW_RESULTS: graph_list,
                VECTOR_RAW_RESULTS: vector_list,
                COVERAGE_DIAGNOSTICS: diagnostics,
                DISAMBIGUATION_RESULT: disambiguation,
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
