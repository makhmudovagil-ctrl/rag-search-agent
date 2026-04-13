"""
RAG Search Agent — main entry point.

Pipeline: Router → ConditionalScout → Synthesizer

All three stages run inside a single BaseAgent so that only the final
synthesized answer is visible to the user in Playground.
"""

import json
import logging
from typing import AsyncGenerator

from google.adk.agents import BaseAgent, LlmAgent
from google.adk.agents.invocation_context import InvocationContext
from google.adk.events import Event, EventActions
from google.genai import Client
from google.genai.types import Content, Part, GenerateContentConfig

from VertexRAGSearchAgent.agents.reranker import merge_and_dedup, run_reranker
from VertexRAGSearchAgent.agents.router import RoutingDecision, ROUTER_INSTRUCTION
from VertexRAGSearchAgent.agents.scout import ConditionalScoutAgent
from VertexRAGSearchAgent.agents.synthesizer import _build_instruction as build_synth_instruction
from VertexRAGSearchAgent.state import (
    GRAPH_RAW_RESULTS,
    RERANKED_RESULTS,
    ROUTING_DECISION,
    ROUTING_STRATEGY,
    VECTOR_RAW_RESULTS,
)

logger = logging.getLogger(__name__)


class RAGSearchAgent(BaseAgent):
    """Single agent that runs Router → Scout → Synthesizer internally.

    Only the Synthesizer's final answer is emitted as a visible event.
    """

    name: str = "rag_search_agent"
    description: str = (
        "Expert discovery pipeline with intent-based routing, "
        "graph/vector search, and result synthesis."
    )
    model: str = "gemini-2.0-flash"

    async def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[Event, None]:
        # Extract user query from the last user message
        user_query = ""
        if ctx.user_content and ctx.user_content.parts:
            user_query = "".join(p.text for p in ctx.user_content.parts if p.text)

        if not user_query:
            yield self._text_event(ctx, "No query provided.")
            return

        # ── Step 1: Router (direct Gemini call, no visible output) ──────────
        client = Client(vertexai=True)
        router_response = client.models.generate_content(
            model=self.model,
            contents=[Content(parts=[Part(text=user_query)], role="user")],
            config=GenerateContentConfig(
                system_instruction=ROUTER_INSTRUCTION,
                response_mime_type="application/json",
                response_schema=RoutingDecision,
            ),
        )

        try:
            routing = json.loads(router_response.text)
        except (json.JSONDecodeError, AttributeError):
            routing = {
                "strategy": "graph",
                "reasoning": "Failed to parse routing, defaulting to graph",
                "confidence": 0.5,
                "search_params": [],
            }

        # If strategy is "vector" but we have keyword params, also try graph
        if routing["strategy"] == "vector":
            has_keywords = any(
                (sp.get("param_type") if isinstance(sp, dict) else sp.param_type) == "keyword"
                for sp in routing.get("search_params", [])
            )
            if has_keywords:
                routing["strategy"] = "hybrid"
                routing["reasoning"] += " (upgraded to hybrid — keyword graph search available)"

        # Write routing to state
        ctx.session.state[ROUTING_DECISION] = routing
        ctx.session.state[ROUTING_STRATEGY] = routing["strategy"]

        # ── Step 2: Scout (deterministic, no visible output) ────────────────
        scout = ConditionalScoutAgent()
        async for event in scout.run_async(ctx):
            # Apply state_delta manually — runner doesn't do it for nested calls
            if event.actions and event.actions.state_delta:
                for key, val in event.actions.state_delta.items():
                    ctx.session.state[key] = val

        # ── Step 2.5: Re-ranker (direct Gemini call, no visible output) ─────
        graph_list = ctx.session.state.get(GRAPH_RAW_RESULTS, [])
        vector_list = ctx.session.state.get(VECTOR_RAW_RESULTS, [])
        merged = merge_and_dedup(graph_list, vector_list)

        if merged:
            reranked = run_reranker(client, self.model, user_query, merged)
        else:
            reranked = []
        ctx.session.state[RERANKED_RESULTS] = reranked

        # ── Step 3: Synthesizer (direct Gemini call, visible output) ────────
        # Build instruction with injected state data
        class FakeCtx:
            """Minimal context to pass state to the synthesizer instruction builder."""
            def __init__(self, state):
                self.state = state
        synth_instruction = build_synth_instruction(FakeCtx(ctx.session.state))

        synth_response = client.models.generate_content(
            model=self.model,
            contents=[Content(parts=[Part(text=user_query)], role="user")],
            config=GenerateContentConfig(
                system_instruction=synth_instruction,
            ),
        )

        yield self._text_event(ctx, synth_response.text)

    def _text_event(self, ctx: InvocationContext, text: str) -> Event:
        return Event(
            invocation_id=ctx.invocation_id,
            author=self.name,
            branch=ctx.branch,
            content=Content(parts=[Part(text=text)]),
            actions=EventActions(),
        )


def create_agent(model: str = "gemini-2.0-flash") -> RAGSearchAgent:
    """Create the RAG search pipeline agent."""
    return RAGSearchAgent(model=model)


# Root agent for ADK / Agent Engine deployment
root_agent = create_agent()
