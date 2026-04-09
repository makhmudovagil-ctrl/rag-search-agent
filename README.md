# RAG Search Agent — Spanner Migration & Knowledge Graph

Copy Knowledge Graph databases from the source project (read-only) to the agent project (full permissions), apply schema changes, and deploy the RAG search agent.

## Source

- **Project:** `kg-poc-489015`
- **Instance:** `kg-poc-instance` (europe-west1)
- **Databases:** `kg_products_v4`, `kg_v2_3`

## Destination

- **Project:** `gcp-poc-488614`
- **Instance:** `kg-dev-instance` (us-central1, **Enterprise** edition, 100 PU)
- **Databases:** `kg_products_v4_dev`, `kg_v2_3_dev`

## Prerequisites

```bash
pip install -r requirements.txt
```

## Migration Steps (run in order)

```bash
# 1. Create Spanner instance (Enterprise edition) + database (tables only, no Property Graph)
python 01_create_instance_and_db.py

# 2. Copy all data from source to destination (~99K rows, ~60s)
python 02_copy_data.py

# 3. Apply Property Graph definition (requires data to be loaded first)
python 03_apply_property_graph.py

# 4. Verify — compare row counts between source and destination
python 04_verify.py

# 5. Apply schema changes (P0/P1/P2 from planning.md Part 2)
#    - Data cleanup, coverage metadata, disambiguation, routing signals,
#      keyword edges, embeddings columns
python 05_schema_changes.py

# 6. Copy kg_v2_3 database (~2.4M rows, ~46 min)
#    - Creates kg_v2_3_dev, copies data, creates indexes, applies Property Graph
python 06_copy_kg_v2_3.py
```

## Project Structure

```
├── 01_create_instance_and_db.py   # Create Spanner instance + kg_products_v4_dev
├── 02_copy_data.py                # Copy kg_products_v4 data
├── 03_apply_property_graph.py     # Apply Property Graph to kg_products_v4_dev
├── 04_verify.py                   # Verify row counts match source
├── 05_schema_changes.py           # Schema upgrades (P0/P1/P2)
├── 06_copy_kg_v2_3.py             # Copy kg_v2_3 → kg_v2_3_dev (full pipeline)
├── scripts/
│   └── deploy_agent_engine.py     # Example script for ADK agent deployment
├── spanner_migration/             # Migration package
├── planning.md                    # Full implementation plan (Parts 1-3)
├── requirements.txt               # Agent Engine dependencies
├── PROGRESS.md                    # Detailed execution log
└── CLAUDE.md                      # Claude Code instructions
```

## Important Notes

- **Enterprise edition** is required for Property Graph (Standard doesn't support it)
- Cost: ~$117/month for 100 PU Enterprise (both databases share the same instance)
- `02_copy_data.py` and `06_copy_kg_v2_3.py` use `snapshot.read()` API (not SQL) to avoid column name conflicts
- Both copy scripts use `insert_or_update` — safe to re-run
- `05_schema_changes.py` DDL steps are idempotent only on first run; to re-run from scratch, drop and recreate the database first
- `06_copy_kg_v2_3.py` index creation may time out in Python but Spanner completes it in background

## Progress

See `PROGRESS.md` for detailed execution log.
See `planning.md` for full implementation plan (Parts 1-3).
