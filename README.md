# Vertex RAG Search Agent

An expert discovery pipeline that finds domain experts across professional networks using a hybrid retrieval strategy combining **Spanner Property Graph (GQL)** for structural queries and **Vertex AI Vector Search** for semantic discovery. Built with Google ADK, deployed to Vertex AI Agent Engine.

## Architecture

The agent follows a **Router → Scout → Re-ranker → Synthesizer** pipeline. Only the Synthesizer's final answer is visible to the user.

```mermaid
graph TD
    Q((User Query)) --> Router

    subgraph "Step 1: Intent Classification"
        Router["Router — Gemini structured output<br/>(strategy + search params)"]
        Router -->|graph| GS
        Router -->|vector| VS
        Router -->|hybrid| GS & VS
        Router -->|temporal| TS
    end

    subgraph "Step 2: Retrieval — ConditionalScoutAgent"
        GS["Graph Search<br/>9 GQL functions"]
        VS["Vector Search<br/>COSINE_DISTANCE over 2 corpora"]
        TS["Temporal Search<br/>churn detection"]
        KW["Keyword Expansion<br/>keyword → product / industry / function"]

        GS -->|"< 3 results"| VS
        GS --> Disambig["Entity Disambiguation<br/>ambiguity check + aliases"]
        GS -->|"< 3 results"| Diag["Coverage Diagnostics<br/>sparse result explainer"]
        KW -.->|discovered entities| GS
    end

    subgraph "Step 2.5: Re-ranking"
        GS & VS & TS --> Reranker["Re-ranker — Gemini<br/>scores 0–1 by relevance,<br/>recency, seniority, supply chain"]
    end

    subgraph "Step 3: Synthesis"
        Reranker --> Synth["Synthesizer — Gemini<br/>dedup, format, coverage estimate"]
        Disambig -.-> Synth
        Diag -.-> Synth
    end

    Synth --> A((Final Answer))
```

## Key Components

### 1. Router (`agents/router.py`)

Gemini-powered intent classifier with structured output (`RoutingDecision` schema). Determines the optimal retrieval strategy before any data is fetched.

- **Strategy selection**: `graph`, `vector`, or `hybrid` based on query signals
- **Parameter extraction**: Product, Company, Industry, Function, Seniority, Supply Chain Position
- **Temporal detection**: Recognizes time-based signals ("who left in the last year") and routes to temporal search
- **Auto-upgrade**: Promotes `vector` → `hybrid` when keyword parameters are present

### 2. ConditionalScoutAgent (`agents/scout.py`)

Deterministic `BaseAgent` (no LLM) that reads the routing decision from session state and dispatches search tools:

- **Graph path** → runs graph search → if results < 3 → automatic vector fallback
- **Vector path** → runs vector search only
- **Hybrid path** → runs graph + vector in parallel
- **Temporal path** → runs churn detection (employment, involvement, relationship)
- **Disambiguation** → checks ambiguous company names, surfaces aliases
- **Coverage diagnostics** → explains why results are sparse ("Company found, but no linked experts")

### 3. Re-ranker (`agents/reranker.py`)

Gemini contextual re-ranking step between Scout and Synthesizer. Merges results from all sources, deduplicates by expert, and scores each expert 0–1 on:

- Query relevance
- Recency (current vs. former roles)
- Seniority level
- Supply chain position match

### 4. Synthesizer (`agents/synthesizer.py`)

Final Gemini call that produces the user-facing answer. Reads re-ranked results from session state and generates a structured response with:

- Expert profiles with evidence-based relevance explanations
- Disambiguation notices (when company names are ambiguous)
- Coverage estimate ("Found X of ~Y estimated experts")
- Diagnostic notes (when results are sparse)

## Retrieval Strategies

### Structural Search — Graph (`tools/graph_search.py`)

9 GQL functions over Spanner Property Graph:

| Function | Traversal Path | Use Case |
|----------|---------------|----------|
| `search_experts_by_product` | Expert → Employment → Product | "Who works with SAP ERP?" |
| `search_experts_by_company` | Expert → Employment → Company | "Experts at Shell" |
| `search_experts_by_industry` | Expert → Employment → Company → Industry | "Oil & gas experts" |
| `search_experts_by_function` | Expert → Employment → Role | "VP-level finance people" |
| `search_experts_by_keyword` | Keyword → Artifact → Employment → Expert | "SCADA specialists" |
| `expand_keyword_to_experts` | Keyword → Product/Industry/Function → Expert | Keyword expansion |
| `search_experts_multi_hop` | Combined product + company + industry + function | Complex queries |
| `get_expert_profile` | Full employment history for one expert | Profile lookup |
| `get_coverage_diagnostics` | Entity existence + expert/artifact counts | Sparse result analysis |

Additional tools: `find_recent_churn()` (temporal search), `check_company_disambiguation()` (entity disambiguation).

### Semantic Search — Vector (`tools/vector_search.py`)

Vertex AI `text-embedding-005` (768 dimensions) with `COSINE_DISTANCE` in Spanner. Two embedding corpora searched via UNION ALL:

| Corpus | Source | Use Case |
|--------|--------|----------|
| `knowledge_artifact.text_embedding` | Published articles, papers, analyses | Domain expertise signals |
| `employment_record.responsibilities_embedding` | Job responsibility descriptions | Duty-oriented queries ("managed P&L", "oversaw operations") |

Results are deduplicated by expert in Python (over-fetch `limit × 10`) and tagged with `match_source` for evidence attribution.

## Technical Stack

| Component | Technology |
|-----------|-----------|
| Framework | Google ADK (Agent Development Kit) |
| LLM | Gemini 2.0 Flash |
| Database | Google Cloud Spanner — Property Graph + GQL |
| Embeddings | Vertex AI `text-embedding-005` (768 dims) |
| Validation | Pydantic >= 2.7 |
| Deployment | Vertex AI Agent Engine |

## Status

| Phase | Features | Status |
|-------|----------|--------|
| **Phase 1** | Vector search, sparse result explainer, entity disambiguator, embedding generation | ✅ Complete |
| **Phase 2** | Re-ranker, keyword expansion, temporal tool, coverage estimation | ✅ Complete |
| **Phase 3** | Multi-turn sessions, query decomposition, schema lookup, A2A integration | Planned |
