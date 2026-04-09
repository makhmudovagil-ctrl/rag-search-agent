"""
Router Agent — Pattern B (Structured Output Schema).

Classifies user query into a retrieval strategy BEFORE any tool calls.
Emits: {strategy, reasoning, named_entities, semantic_signals, search_params}.
"""

from pydantic import BaseModel, Field
from google.adk.agents import LlmAgent

from VertexRAGSearchAgent.state import ROUTING_DECISION, ROUTING_STRATEGY


# ── Output schema ──────────────────────────────────────────────────────────────

class SearchParam(BaseModel):
    """A single structured search parameter extracted from the query."""
    param_type: str = Field(
        description="Type of parameter: product, company, industry, function, "
        "seniority, supply_chain_position, keyword, is_current_role"
    )
    value: str = Field(description="Extracted value for this parameter")


class RoutingDecision(BaseModel):
    """Structured routing decision emitted before tool calls."""
    strategy: str = Field(
        description='Retrieval strategy: "graph", "vector", or "hybrid"'
    )
    reasoning: str = Field(
        description="Brief explanation of why this strategy was chosen"
    )
    confidence: float = Field(
        description="Confidence in routing decision (0.0 to 1.0)",
        ge=0.0, le=1.0,
    )
    named_entities: list[str] = Field(
        default_factory=list,
        description="Named entities detected in query: company names, product names, "
        "industry names, person names",
    )
    semantic_signals: list[str] = Field(
        default_factory=list,
        description="Semantic/behavioral language detected: verbs like 'navigated', "
        "'transitioned', 'managed', 'experienced'",
    )
    search_params: list[SearchParam] = Field(
        default_factory=list,
        description="Structured search parameters extracted from the query",
    )


# ── Router instruction ─────────────────────────────────────────────────────────

ROUTER_INSTRUCTION = """You are the routing component of an expert discovery pipeline.
Your job is to analyze the user's query and decide the optimal retrieval strategy.

## Strategy Decision Rules

Choose **"graph"** when:
- Query contains named entities (specific companies, products, people)
- Query asks about organizational structure, roles, seniority
- Query asks count or coverage questions ("how many experts...")
- Query references supply chain positions (buyer, seller, evaluator)
- Query asks about industry/function combinations

Choose **"vector"** when:
- Query uses semantic/behavioral language ("experienced in navigating", "managed transition")
- Query describes expertise conceptually rather than structurally
- Query asks about themes, trends, or qualitative experience
- No specific named entities that can anchor a graph traversal

Choose **"hybrid"** when:
- Query has BOTH structural anchors AND semantic qualifiers
- Example: "Salesforce buyers who navigated a CRM migration" (Salesforce=graph, navigated migration=vector)

## Search Parameters Extraction

Extract structured parameters for the graph search:
- **product**: Specific product names (e.g., "Salesforce", "SAP S/4HANA")
- **company**: Company names (e.g., "Google", "Deloitte")
- **industry**: Industry names (e.g., "Financial Services", "Health Care")
- **function**: Role functions — ONLY these 11 values: Operations, Commercial, Strategy, Finance, Engineering, IT, R&D, Procurement, Legal, HR, C-Suite
- **seniority**: Seniority level (Senior, Mid-Level, Junior, C-Suite)
- **supply_chain_position**: ONLY these 10 values: buyer, seller, user, evaluator, advisor, analyst, operator, competitor, neutral, none
- **keyword**: Semantic keywords for keyword-based graph search
- **is_current_role**: "true" or "false" — whether to filter by current employment

## Available Knowledge Graph Schema

Node tables: Expert, Company, Product, Industry, Subindustry, Role, EmploymentRecord, Keyword, KnowledgeArtifact, CompetitiveSet, ProductCategory, Geography, Project, Angle, CompanyAlias

Key traversal paths:
- Expert → EmploymentRecord → Product (via INVOLVED_WITH)
- Expert → EmploymentRecord → Company → Industry
- Expert → EmploymentRecord → Role (function, seniority)
- Keyword → KnowledgeArtifact → EmploymentRecord → Expert

Analyze the query and emit your routing decision.
"""


# ── Agent definition ───────────────────────────────────────────────────────────

def create_router_agent(model: str = "gemini-2.0-flash") -> LlmAgent:
    """Create the router LLM agent."""
    return LlmAgent(
        name="router",
        description="Analyzes user queries and decides the optimal retrieval strategy "
        "(graph, vector, or hybrid) before any tool calls.",
        model=model,
        instruction=ROUTER_INSTRUCTION,
        output_schema=RoutingDecision,
        output_key=ROUTING_DECISION,
    )
