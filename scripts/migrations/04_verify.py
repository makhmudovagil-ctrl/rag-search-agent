"""
Step 4: Verify data copy — compare row counts between source and destination.

Prints a side-by-side table showing row counts for every table.
Exits with code 1 if any mismatch is found.
"""

from google.cloud import spanner

# ── Config ──────────────────────────────────────────────────────────
SRC_PROJECT = "kg-poc-489015"
SRC_INSTANCE = "kg-poc-instance"
SRC_DATABASE = "kg_products_v4"

DST_PROJECT = "gcp-poc-488614"
DST_INSTANCE = "kg-dev-instance"
DST_DATABASE = "kg_products_v4_dev"

TABLES = [
    "angle", "company", "competitive_set", "employment_record", "expert",
    "geography", "industry", "keyword", "knowledge_artifact", "product",
    "product_category", "project", "role", "subindustry",
    "edge_angle_targets_company", "edge_angle_targets_industry",
    "edge_angle_targets_product", "edge_angle_targets_subindustry",
    "edge_at_company", "edge_based_in_country", "edge_belongs_to_category",
    "edge_competitive_set_for", "edge_competitor_of", "edge_covers",
    "edge_customer_of", "edge_features", "edge_for_angle",
    "edge_for_project", "edge_has_employment", "edge_has_role",
    "edge_in_competitive_set", "edge_in_industry", "edge_in_subindustry",
    "edge_involved_with", "edge_maps_to_industry", "edge_maps_to_product",
    "edge_mentioned_in", "edge_produced_by", "edge_project_has_angle",
    "edge_relevant_employment", "edge_selected_for", "edge_subindustry_of",
    "edge_supplier_of",
]


def get_count(database, table_name: str) -> int:
    with database.snapshot() as snapshot:
        result = snapshot.execute_sql(f"SELECT COUNT(*) FROM `{table_name}`")
        return list(result)[0][0]


def main():
    src_client = spanner.Client(project=SRC_PROJECT)
    dst_client = spanner.Client(project=DST_PROJECT)

    src_db = src_client.instance(SRC_INSTANCE).database(SRC_DATABASE)
    dst_db = dst_client.instance(DST_INSTANCE).database(DST_DATABASE)

    print(f"{'Table':<40} {'Source':>10} {'Dest':>10} {'Status':>10}")
    print("-" * 74)

    mismatches = 0
    total_src = 0
    total_dst = 0

    for table in TABLES:
        try:
            src_count = get_count(src_db, table)
            dst_count = get_count(dst_db, table)
        except Exception as e:
            print(f"{table:<40} {'ERROR':>10} {'ERROR':>10} {'❌':>10}  {e}")
            mismatches += 1
            continue

        total_src += src_count
        total_dst += dst_count
        status = "✅" if src_count == dst_count else "❌ MISMATCH"
        if src_count != dst_count:
            mismatches += 1
        print(f"{table:<40} {src_count:>10} {dst_count:>10} {status:>10}")

    print("-" * 74)
    print(f"{'TOTAL':<40} {total_src:>10} {total_dst:>10}")
    print()

    if mismatches:
        print(f"❌ {mismatches} table(s) with mismatches!")
        exit(1)
    else:
        print("✅ All tables match — copy verified successfully!")


if __name__ == "__main__":
    main()
