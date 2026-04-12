"""Session state keys for the RAG Search Agent pipeline."""


# Routing decision from the Router agent
ROUTING_STRATEGY = "rag:routing_strategy"          # "graph" | "vector" | "hybrid"
ROUTING_DECISION = "rag:routing_decision"          # full routing dict

# Raw retrieval results (before re-ranking)
GRAPH_RAW_RESULTS = "rag:graph_raw_results"        # list[dict]
VECTOR_RAW_RESULTS = "rag:vector_raw_results"      # list[dict]

# Coverage diagnostics (P1.3 — sparse result explainer)
COVERAGE_DIAGNOSTICS = "rag:coverage_diagnostics"   # dict from get_coverage_diagnostics()

# Multi-turn context
RETRIEVAL_HISTORY = "rag:retrieval_history"         # list[dict] per turn
SCHEMA_CACHE = "rag:schema_cache"                  # cached schema lookups
ACTIVE_STRATEGY = "rag:active_strategy"            # biased strategy from recent turns
