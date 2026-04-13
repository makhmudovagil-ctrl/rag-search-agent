"""
Step 10: Classify keywords into role functions and product categories.

Populates D3 (edge_maps_to_function) and D4 (edge_maps_to_product_category)
by sending keywords to Gemini in batches for classification.

Uses structured JSON output to map each keyword to zero or more:
  - role functions (from the 11 known functions in the role table)
  - product categories (from the 31 known categories in product_category)

Idempotent: skips keywords that already have entries in both tables.

Usage:
  python scripts/migrations/10_classify_keywords.py --dry-run
  python scripts/migrations/10_classify_keywords.py
  python scripts/migrations/10_classify_keywords.py --batch-size 50
  python scripts/migrations/10_classify_keywords.py --limit 100

Environment:
  GCP_PROJECT_ID        default: gcp-poc-488614
  GCP_REGION            default: us-central1
  SPANNER_INSTANCE_ID   default: kg-dev-instance
  SPANNER_DATABASE_ID   default: kg_products_v4_dev
"""

import argparse
import json
import os
import sys
import time

import vertexai
from google.cloud import spanner
from google.genai import Client

# ── Config ──────────────────────────────────────────────────────────
PROJECT = os.getenv("GCP_PROJECT_ID", "gcp-poc-488614")
REGION = os.getenv("GCP_REGION", "us-central1")
INSTANCE = os.getenv("SPANNER_INSTANCE_ID", "kg-dev-instance")
DATABASE = os.getenv("SPANNER_DATABASE_ID", "kg_products_v4_dev")

MODEL = "gemini-2.0-flash"
BATCH_SIZE = 100

# Known role functions (from role.function distinct values)
ROLE_FUNCTIONS = [
    "C-Suite", "Commercial", "Engineering", "Finance", "HR",
    "IT", "Legal", "Operations", "Procurement", "R&D", "Strategy",
]

# Known product category IDs (from product_category table)
PRODUCT_CATEGORIES = [
    "application_development_management",
    "automated_process_workflow_systems",
    "business_process_management_bpm",
    "case_management",
    "cloud_infrastructure_computing",
    "collaboration",
    "commerce",
    "communications_technology",
    "construction",
    "customer_relationship_management_crm",
    "database_management_software",
    "electronic_data_interchange_edi",
    "enterprise_business_solutions_ebs",
    "enterprise_resource_planning_erp",
    "financial_analytical_applications",
    "hr_management_systems_hrms_human_capital_management_hcm",
    "it_infrastructure_operations_management",
    "information_technology_management",
    "inventory_management",
    "legal_and_professional_services_management",
    "marketing_performance_measurement",
    "middleware_software",
    "platform_as_a_service_paas",
    "procurement",
    "product_lifecycle_management_plm",
    "productivity_solutions",
    "service_field_support_management",
    "software_other",
    "software_configuration_management_scm",
    "supplier_relationship_management_srm",
    "supply_chain_management_scm",
]

CLASSIFICATION_PROMPT = f"""You are a keyword classifier for an expert discovery knowledge graph.

Given a list of keywords, classify each one into:
1. **Role functions** it relates to (zero or more from the list below)
2. **Product categories** it relates to (zero or more from the list below)

For each match, provide a confidence score (0.0 to 1.0). Only include matches with confidence >= 0.5.

## Valid Role Functions
{json.dumps(ROLE_FUNCTIONS)}

## Valid Product Category IDs
{json.dumps(PRODUCT_CATEGORIES)}

## Rules
- A keyword can map to MULTIPLE functions and/or categories.
- If a keyword is too generic or unrelated (e.g., "3g", "1040s"), return empty arrays.
- Use the exact function names and category IDs from the lists above.
- Be liberal with classification — if there's a reasonable connection, include it.
- Return valid JSON only.

## Output Format
Return a JSON object where keys are keyword_ids and values have "functions" and "categories":
```json
{{
  "keyword_id_1": {{
    "functions": [{{"function": "Procurement", "confidence": 0.9}}],
    "categories": [{{"category_id": "supply_chain_management_scm", "confidence": 0.8}}]
  }},
  "keyword_id_2": {{
    "functions": [],
    "categories": []
  }}
}}
```
"""


def get_db():
    """Get Spanner database handle."""
    client = spanner.Client(project=PROJECT)
    instance = client.instance(INSTANCE)
    return instance.database(DATABASE)


def load_all_keywords(db) -> list[tuple[str, str]]:
    """Load all keyword_id, keyword pairs from Spanner."""
    with db.snapshot() as s:
        rows = list(s.read(
            "keyword",
            columns=["keyword_id", "keyword"],
            keyset=spanner.KeySet(all_=True),
        ))
    return [(r[0], r[1]) for r in rows]


def load_existing_function_mappings(db) -> set[str]:
    """Load keyword_ids that already have function mappings."""
    with db.snapshot() as s:
        rows = list(s.execute_sql(
            "SELECT DISTINCT keyword_id FROM edge_maps_to_function"
        ))
    return {r[0] for r in rows}


def load_existing_category_mappings(db) -> set[str]:
    """Load keyword_ids that already have category mappings."""
    with db.snapshot() as s:
        rows = list(s.execute_sql(
            "SELECT DISTINCT keyword_id FROM edge_maps_to_product_category"
        ))
    return {r[0] for r in rows}


def classify_batch(
    client: Client,
    keywords: list[tuple[str, str]],
) -> dict:
    """Send a batch of keywords to Gemini for classification.

    Args:
        client: Google GenAI client.
        keywords: List of (keyword_id, keyword_text) tuples.

    Returns:
        Dict mapping keyword_id to classification results.
    """
    keyword_list = "\n".join(
        f'- {kid}: "{ktext}"' for kid, ktext in keywords
    )
    user_content = f"Classify these keywords:\n\n{keyword_list}"

    response = client.models.generate_content(
        model=MODEL,
        contents=user_content,
        config={
            "system_instruction": CLASSIFICATION_PROMPT,
            "response_mime_type": "application/json",
        },
    )

    return json.loads(response.text)


def write_function_mappings(db, mappings: list[tuple[str, str, float]]) -> int:
    """Write keyword → function mappings to Spanner.

    Args:
        db: Spanner database.
        mappings: List of (keyword_id, role_function, confidence) tuples.

    Returns:
        Number of rows written.
    """
    if not mappings:
        return 0

    def _insert(txn):
        txn.insert(
            "edge_maps_to_function",
            columns=["keyword_id", "role_function", "confidence"],
            values=mappings,
        )

    db.run_in_transaction(_insert)
    return len(mappings)


def write_category_mappings(db, mappings: list[tuple[str, str, float]]) -> int:
    """Write keyword → category mappings to Spanner.

    Args:
        db: Spanner database.
        mappings: List of (keyword_id, product_category_id, confidence) tuples.

    Returns:
        Number of rows written.
    """
    if not mappings:
        return 0

    def _insert(txn):
        txn.insert(
            "edge_maps_to_product_category",
            columns=["keyword_id", "product_category_id", "confidence"],
            values=mappings,
        )

    db.run_in_transaction(_insert)
    return len(mappings)


def main():
    parser = argparse.ArgumentParser(description="Classify keywords via Gemini")
    parser.add_argument("--dry-run", action="store_true", help="Print but don't write")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="Keywords per Gemini call")
    parser.add_argument("--limit", type=int, default=0, help="Max keywords to process (0 = all)")
    args = parser.parse_args()

    db = get_db()
    genai_client = Client(vertexai=True, project=PROJECT, location=REGION)

    print(f"Loading keywords from {INSTANCE}/{DATABASE}...")
    all_keywords = load_all_keywords(db)
    print(f"  Total keywords: {len(all_keywords)}")

    # Load existing mappings to skip
    existing_fn = load_existing_function_mappings(db)
    existing_cat = load_existing_category_mappings(db)
    already_done = existing_fn & existing_cat
    print(f"  Already classified (both tables): {len(already_done)}")

    # Filter to unclassified keywords
    to_classify = [(kid, kt) for kid, kt in all_keywords if kid not in already_done]
    if args.limit > 0:
        to_classify = to_classify[:args.limit]
    print(f"  To classify: {len(to_classify)}")

    if not to_classify:
        print("Nothing to do.")
        return

    total_fn = 0
    total_cat = 0
    total_skipped = 0
    batches = [to_classify[i:i + args.batch_size] for i in range(0, len(to_classify), args.batch_size)]

    for batch_idx, batch in enumerate(batches):
        print(f"\nBatch {batch_idx + 1}/{len(batches)} ({len(batch)} keywords)...")

        try:
            result = classify_batch(genai_client, batch)
        except Exception as e:
            print(f"  ERROR: Gemini call failed: {e}")
            total_skipped += len(batch)
            time.sleep(2)
            continue

        # Parse function mappings
        fn_mappings = []
        cat_mappings = []

        for kid, _ in batch:
            classification = result.get(kid, {})
            if not isinstance(classification, dict):
                continue

            for fn_entry in classification.get("functions", []):
                if not isinstance(fn_entry, dict):
                    continue
                fn_name = fn_entry.get("function", "")
                confidence = fn_entry.get("confidence", 0.0)
                if fn_name in ROLE_FUNCTIONS and confidence >= 0.5:
                    if kid not in existing_fn:
                        fn_mappings.append((kid, fn_name, confidence))

            for cat_entry in classification.get("categories", []):
                if not isinstance(cat_entry, dict):
                    continue
                cat_id = cat_entry.get("category_id", "")
                confidence = cat_entry.get("confidence", 0.0)
                if cat_id in PRODUCT_CATEGORIES and confidence >= 0.5:
                    if kid not in existing_cat:
                        cat_mappings.append((kid, cat_id, confidence))

        if args.dry_run:
            print(f"  [DRY RUN] Would write {len(fn_mappings)} function + {len(cat_mappings)} category mappings")
            for kid, fn, conf in fn_mappings[:5]:
                print(f"    D3: {kid} → {fn} ({conf:.2f})")
            for kid, cat, conf in cat_mappings[:5]:
                print(f"    D4: {kid} → {cat} ({conf:.2f})")
        else:
            # Write in smaller chunks to avoid transaction size limits
            chunk_size = 500
            for i in range(0, len(fn_mappings), chunk_size):
                chunk = fn_mappings[i:i + chunk_size]
                written = write_function_mappings(db, chunk)
                total_fn += written

            for i in range(0, len(cat_mappings), chunk_size):
                chunk = cat_mappings[i:i + chunk_size]
                written = write_category_mappings(db, chunk)
                total_cat += written

            print(f"  Wrote {len(fn_mappings)} function + {len(cat_mappings)} category mappings")

        # Rate limit
        if batch_idx < len(batches) - 1:
            time.sleep(1)

    print(f"\n{'=' * 50}")
    print(f"DONE. Total function mappings: {total_fn}, category mappings: {total_cat}")
    if total_skipped:
        print(f"  Skipped (errors): {total_skipped}")
    print(f"  Re-run to retry any failed batches.")


if __name__ == "__main__":
    main()
