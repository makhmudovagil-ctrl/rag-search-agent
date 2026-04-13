"""
Step 12: Load data from data-v5/ JSON files into kg_products_v5_dev.

Reads all nodes_*.json and edges_*.json files from data-v5/ directory.
Handles type coercions: JSON arrays/dicts → STRING(MAX), None → NULL.
Tables are loaded in dependency order: node tables first, then edge tables.
"""

import json
import os
import time

from google.cloud import spanner

# ── Config ───────────────────────────────────────────────────────────
PROJECT = "gcp-poc-488614"
INSTANCE = "kg-dev-instance"
DATABASE = "kg_products_v5_dev"

DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data-v5",
)

BATCH_SIZE = 500  # rows per commit (safe for wide tables with STRING(MAX) columns)

# Tables in dependency order: node tables first, then edges.
# Each entry: (spanner_table_name, json_file_name, [column_names_in_json_order])
# Tables in dependency order: node tables first, then edges.
# Each entry: (spanner_table, json_file, [columns], {bool_columns})
# bool_columns: field names that must be coerced to BOOL regardless of JSON type
BOOL_COERCE: dict[str, set[str]] = {
    "expert":          {"is_active"},
    "employment_record": {"is_current"},
    "project":         {"software_focus"},
    "angle":           {"customers_of_providers"},
    "edge_involved_with": {"is_user", "is_evaluator", "is_key_decision_maker"},
    "edge_customer_of":  {"validated"},
    "edge_supplier_of":  {"validated"},
}

TABLES = [
    # ── Node tables ──────────────────────────────────────────────────
    ("expert", "nodes_expert.json", [
        "expert_id", "name", "is_active", "skills",
    ]),
    ("company", "nodes_company.json", [
        "company_id", "name_raw", "name_normalised", "linkedin_url",
        "website", "size", "founded", "description", "market_position",
        "company_type",
    ]),
    ("employment_record", "nodes_employment.json", [
        "employment_id", "expert_id", "company_id", "company",
        "position", "jobtitle_raw", "start_date", "end_date",
        "start_year", "start_month", "end_year", "end_month",
        "is_current", "responsibilities", "geo",
    ]),
    ("role", "nodes_role.json", [
        "role_id", "role_name", "function", "seniority", "org_scope",
    ]),
    ("industry", "nodes_industry.json", [
        "industry_id", "name", "description",
    ]),
    ("subindustry", "nodes_subindustry.json", [
        "subindustry_id", "name", "definition", "industry_ids",
    ]),
    ("product", "nodes_product.json", [
        "product_id", "product_name", "vendor_name",
        "vendor_company_id", "source_artifact_id",
    ]),
    ("product_category", "nodes_product_category.json", [
        "product_category_id", "name", "category_parent",
    ]),
    ("geography", "nodes_geography.json", [
        "geography_id", "name", "level", "parent_id",
    ]),
    ("keyword", "nodes_keyword.json", [
        "keyword_id", "term_string", "source",
    ]),
    ("knowledge_artifact", "nodes_artifact.json", [
        "artifact_id", "artifact_type", "date", "call_id", "text",
        "lens", "lens_rationale", "expert_id", "project_id",
    ]),
    ("competitive_set", "nodes_competitive_set.json", [
        "cs_id", "perspective", "customer_segment", "use_cases",
        "validated_in_artifact_id", "project_id",
    ]),
    ("project", "nodes_project.json", [
        "project_id", "name", "created_at", "brief_object",
        "project_type_raw", "software_focus", "client_company_id",
        "client_company_name", "brief_dimension",
    ]),
    ("angle", "nodes_angle.json", [
        "angle_id", "name", "type", "tenure", "scope",
        "angle_bio", "priority", "customers_of_providers",
    ]),

    # ── Edge tables ──────────────────────────────────────────────────
    ("edge_has_employment", "edges_has_employment.json", [
        "expert_id", "employment_id",
    ]),
    ("edge_at_company", "edges_at_company.json", [
        "employment_id", "company_id",
    ]),
    ("edge_in_role", "edges_in_role.json", [
        "employment_id", "role_id",
    ]),
    ("edge_based_in", "edges_based_in.json", [
        "employment_id", "geography_id",
    ]),
    ("edge_covers", "edges_covers.json", [
        "employment_id", "geography_id",
    ]),
    ("edge_in_industry", "edges_in_industry.json", [
        "company_id", "industry_id",
    ]),
    ("edge_in_subindustry", "edges_in_subindustry.json", [
        "company_id", "subindustry_id",
    ]),
    ("edge_involved_with", "edges_involved_with.json", [
        "employment_id", "product_id", "source_artifact_id",
        "is_user", "is_evaluator", "is_key_decision_maker",
        "supply_chain_position", "start_date", "end_date",
        "attachment_method", "validation_source", "notes",
    ]),
    ("edge_competitor_of", "edges_competitor_of.json", [
        "company_id_a", "company_id_b", "source", "source_artifact_id",
    ]),
    ("edge_customer_of", "edges_customer_of.json", [
        "buyer_company_id", "seller_company_id", "status", "end_date",
        "validated_in_artifact_id", "validated",
    ]),
    ("edge_supplier_of", "edges_supplier_of.json", [
        "supplier_company_id", "buyer_company_id", "status", "end_date",
        "validated_in_artifact_id", "validated",
    ]),
    ("edge_produced_by", "edges_produced_by.json", [
        "product_id", "company_id",
    ]),
    ("edge_maps_to_product", "edges_maps_to_product.json", [
        "keyword_id", "product_id", "match_method",
    ]),
    ("edge_mentioned_in", "edges_mentioned_in.json", [
        "keyword_id", "artifact_id",
    ]),
    ("edge_mentions_product", "edges_mentions_product.json", [
        "artifact_id", "product_id",
    ]),
    ("edge_belongs_to_category", "edges_belongs_to_category.json", [
        "product_id", "category_id",
    ]),
    ("edge_in_competitive_set", "edges_in_competitive_set.json", [
        "product_id", "cs_id",
    ]),
    ("edge_for_project", "edges_for_project.json", [
        "artifact_id", "project_id",
    ]),
    ("edge_for_angle", "edges_for_angle.json", [
        "artifact_id", "angle_id",
    ]),
    ("edge_features", "edges_features.json", [
        "expert_id", "artifact_id",
    ]),
    ("edge_relevant_employment_mentioned", "edges_relevant_employment_mentioned.json", [
        "artifact_id", "employment_id", "context_type", "matched_employer_text",
    ]),
    ("edge_has_angle", "edges_has_angle.json", [
        "project_id", "angle_id",
    ]),
    ("edge_subindustry_to_industry", "edges_subindustry_to_industry.json", [
        "subindustry_id", "industry_id",
    ]),
    # Large polymorphic edge table — loaded last
    ("edge_keyword_inference", "edges_keyword_inference.json", [
        "keyword_id", "target_type", "target_id", "context_artifact_id",
        "edge_type", "target_name", "confidence",
    ]),
]


def coerce_value(v, force_bool: bool = False):
    """Convert Python value to a Spanner-compatible type.

    JSON arrays/dicts are serialised to STRING(MAX).
    None stays None (NULL). Booleans, ints, floats, strings pass through.
    force_bool: cast the value to Python bool (handles int 0/1, str 'true'/'false').
    """
    if v is None:
        return None
    if force_bool:
        if isinstance(v, str):
            return v.strip().lower() not in ("false", "0", "")
        return bool(v)
    if isinstance(v, (list, dict)):
        return json.dumps(v, ensure_ascii=False)
    return v


def load_table(
    db,
    table_name: str,
    json_file: str,
    columns: list[str],
) -> int:
    """Load a single table from its JSON file.

    Returns number of rows written.
    """
    fpath = os.path.join(DATA_DIR, json_file)
    data = json.load(open(fpath, encoding="utf-8"))

    if not data:
        print(f"  {table_name}: 0 rows (file empty)")
        return 0

    bool_cols = BOOL_COERCE.get(table_name, set())
    total = len(data)
    written = 0

    for i in range(0, total, BATCH_SIZE):
        batch_rows = data[i : i + BATCH_SIZE]
        values = [
            [coerce_value(row.get(col), force_bool=(col in bool_cols)) for col in columns]
            for row in batch_rows
        ]
        with db.batch() as b:
            b.insert_or_update(
                table=table_name,
                columns=columns,
                values=values,
            )
        written += len(batch_rows)

    print(f"  {table_name}: {written} rows written")
    return written


def main():
    client = spanner.Client(project=PROJECT)
    instance = client.instance(INSTANCE)
    db = instance.database(DATABASE)

    print(f"Loading data-v5/ into {PROJECT}/{INSTANCE}/{DATABASE}")
    print(f"Data directory: {DATA_DIR}")
    print(f"Batch size: {BATCH_SIZE} rows\n")

    total_rows = 0
    start = time.time()

    for table_name, json_file, columns in TABLES:
        try:
            count = load_table(db, table_name, json_file, columns)
            total_rows += count
        except Exception as e:
            print(f"  ❌ {table_name}: FAILED — {e}")
            raise

    elapsed = time.time() - start
    print(f"\n✅ Done: {len(TABLES)} tables, {total_rows:,} total rows in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
