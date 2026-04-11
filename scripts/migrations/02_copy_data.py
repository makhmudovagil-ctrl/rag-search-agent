"""
Step 2: Copy all data from source to destination database.

Reads from: kg-poc-489015 / kg-poc-instance / kg_products_v4
Writes to:  gcp-poc-488614 / kg-dev-instance / kg_products_v4_dev

Uses batch mutations for efficient writes (up to 20,000 mutations per commit).
Tables are copied in dependency order — node tables first, then edge tables.
"""

import time

from google.cloud import spanner

# ── Config ──────────────────────────────────────────────────────────
SRC_PROJECT = "kg-poc-489015"
SRC_INSTANCE = "kg-poc-instance"
SRC_DATABASE = "kg_products_v4"

DST_PROJECT = "gcp-poc-488614"
DST_INSTANCE = "kg-dev-instance"
DST_DATABASE = "kg_products_v4_dev"

BATCH_SIZE = 500  # rows per commit (safe for wide tables with STRING(MAX) columns)

# Tables in dependency order: nodes first, then edges.
# Each entry: (table_name, [column_names])
TABLES = [
    # ── Node tables ─────────────────────────────────────────────────
    ("expert", [
        "expert_id", "expert_name", "is_active", "skills",
    ]),
    ("company", [
        "company_id", "company_name", "name_normalised", "linkedin_url",
        "website", "size", "founded", "description", "market_position",
        "company_type",
    ]),
    ("employment_record", [
        "employment_id", "expert_id", "company_id", "position",
        "jobtitle_raw", "start_date", "end_date", "start_year",
        "start_month", "end_year", "end_month", "is_current",
        "responsibilities", "geo",
    ]),
    ("role", [
        "role_id", "role_name", "function", "seniority", "org_scope",
    ]),
    ("industry", [
        "industry_id", "name", "description",
    ]),
    ("subindustry", [
        "subindustry_id", "name", "definition", "industry_ids",
    ]),
    ("product", [
        "product_id", "product_name", "vendor_name", "vendor_company_id",
        "source_artifact_id", "source",
    ]),
    ("product_category", [
        "product_category_id", "name", "category_parent",
    ]),
    ("geography", [
        "geography_id", "name", "level", "parent_id",
    ]),
    ("keyword", [
        "keyword_id", "keyword", "source",
    ]),
    ("knowledge_artifact", [
        "artifact_id", "artifact_type", "date", "call_id", "text",
        "lens", "lens_rationale", "expert_id", "project_id",
        "relevant_employment_id",
    ]),
    ("competitive_set", [
        "competitive_set_id", "project_id", "name", "definition",
        "perspective", "customer_segment", "use_cases",
        "validated_in_artifact_id",
    ]),
    ("project", [
        "project_id", "project_name", "created_at", "brief_object",
        "project_type_raw", "software_focus", "client_company_id",
        "client_company_name", "brief_dimension", "brief_lens",
        "brief_driver",
    ]),
    ("angle", [
        "angle_id", "name", "type", "tenure", "scope", "angle_bio",
        "priority", "customers_of_providers",
    ]),

    # ── Edge tables ─────────────────────────────────────────────────
    ("edge_has_employment", [
        "expert_id", "employment_id",
    ]),
    ("edge_at_company", [
        "employment_id", "company_id",
    ]),
    ("edge_has_role", [
        "employment_id", "role_id",
    ]),
    ("edge_based_in_country", [
        "employment_id", "geography_id",
    ]),
    ("edge_covers", [
        "employment_id", "geography_id",
    ]),
    ("edge_in_industry", [
        "company_id", "industry_id",
    ]),
    ("edge_in_subindustry", [
        "company_id", "subindustry_id",
    ]),
    ("edge_involved_with", [
        "employment_id", "product_id", "source_artifact_id",
        "supply_chain_position", "is_user", "is_evaluator",
        "is_key_decision_maker", "status", "start_date", "end_date",
        "attachment_method", "validation_source", "notes",
    ]),
    ("edge_competitor_of", [
        "company_id_a", "company_id_b", "source",
    ]),
    ("edge_customer_of", [
        "from_company_id", "to_company_id", "status", "end_date",
        "validated_in_artifact_id",
    ]),
    ("edge_supplier_of", [
        "from_company_id", "to_company_id", "status", "end_date",
        "validated_in_artifact_id",
    ]),
    ("edge_selected_for", [
        "expert_id", "project_id", "qualification_id", "angle_id",
        "selection_status", "relevant_employment_id", "selected_at",
    ]),
    ("edge_produced_by", [
        "product_id", "company_id",
    ]),
    ("edge_maps_to_product", [
        "keyword_id", "product_id",
    ]),
    ("edge_maps_to_industry", [
        "keyword_id", "industry_id",
    ]),
    ("edge_mentioned_in", [
        "keyword_id", "artifact_id",
    ]),
    ("edge_belongs_to_category", [
        "product_id", "product_category_id",
    ]),
    ("edge_for_project", [
        "artifact_id", "project_id",
    ]),
    ("edge_for_angle", [
        "artifact_id", "angle_id",
    ]),
    ("edge_features", [
        "artifact_id", "expert_id",
    ]),
    ("edge_relevant_employment", [
        "artifact_id", "employment_id", "context_type",
    ]),
    ("edge_angle_targets_company", [
        "angle_id", "company_id",
    ]),
    ("edge_angle_targets_industry", [
        "angle_id", "industry_id",
    ]),
    ("edge_angle_targets_product", [
        "angle_id", "product_id", "source",
    ]),
    ("edge_angle_targets_subindustry", [
        "angle_id", "subindustry_id",
    ]),
    ("edge_in_competitive_set", [
        "product_id", "competitive_set_id",
    ]),
    ("edge_competitive_set_for", [
        "competitive_set_id", "project_id",
    ]),
    ("edge_project_has_angle", [
        "project_id", "angle_id",
    ]),
    ("edge_subindustry_of", [
        "subindustry_id", "industry_id",
    ]),
]


def copy_table(src_db, dst_db, table_name: str, columns: list[str]) -> int:
    """Read all rows from source table and write to destination in batches.

    Uses snapshot.read() instead of execute_sql() to avoid SQL parsing
    issues when column names clash with table names (e.g. keyword.keyword).
    """
    from google.cloud.spanner_v1 import KeySet

    # Read all rows from source using the read() API (no SQL parsing)
    with src_db.snapshot() as snapshot:
        results = snapshot.read(
            table=table_name,
            columns=columns,
            keyset=KeySet(all_=True),
        )
        rows = list(results)

    if not rows:
        print(f"  {table_name}: 0 rows (empty)")
        return 0

    # Write in batches
    total = len(rows)
    written = 0
    for i in range(0, total, BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        with dst_db.batch() as b:
            b.insert_or_update(
                table=table_name,
                columns=columns,
                values=batch,
            )
        written += len(batch)

    print(f"  {table_name}: {written} rows copied")
    return written


def main():
    src_client = spanner.Client(project=SRC_PROJECT)
    dst_client = spanner.Client(project=DST_PROJECT)

    src_instance = src_client.instance(SRC_INSTANCE)
    dst_instance = dst_client.instance(DST_INSTANCE)

    src_db = src_instance.database(SRC_DATABASE)
    dst_db = dst_instance.database(DST_DATABASE)

    print(f"Source: {SRC_PROJECT}/{SRC_INSTANCE}/{SRC_DATABASE}")
    print(f"Destination: {DST_PROJECT}/{DST_INSTANCE}/{DST_DATABASE}")
    print(f"Batch size: {BATCH_SIZE} rows\n")

    total_rows = 0
    total_tables = 0
    start = time.time()

    for table_name, columns in TABLES:
        try:
            count = copy_table(src_db, dst_db, table_name, columns)
            total_rows += count
            total_tables += 1
        except Exception as e:
            print(f"  ❌ {table_name}: FAILED — {e}")
            raise

    elapsed = time.time() - start
    print(f"\n✅ Done: {total_tables} tables, {total_rows} total rows in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
