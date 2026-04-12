"""
Step 8: Populate company.ambiguity_flag based on name collision heuristic.

Sets ambiguity_flag = true for any company whose LOWER(company_name) matches
2+ distinct company_id values (i.e., multiple companies share the same
normalized name).

Idempotent: resets ALL flags to false, then recomputes from scratch.

Usage:
  python scripts/migrations/08_populate_ambiguity_flags.py
  python scripts/migrations/08_populate_ambiguity_flags.py --dry-run

Environment (falls back to defaults if not set):
  GCP_PROJECT_ID        default: gcp-poc-488614
  SPANNER_INSTANCE_ID   default: kg-dev-instance
  SPANNER_DATABASE_ID   default: kg_products_v4_dev
"""

import argparse
import logging
import os
import sys

import google.auth
from google.cloud import spanner
from google.cloud.spanner_v1.database import Database as SpannerDatabase

# ── Config ──────────────────────────────────────────────────────────
PROJECT = os.getenv("GCP_PROJECT_ID", "gcp-poc-488614")
INSTANCE = os.getenv("SPANNER_INSTANCE_ID", "kg-dev-instance")
DATABASE = os.getenv("SPANNER_DATABASE_ID", "kg_products_v4_dev")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def get_db() -> SpannerDatabase:
    """Connect to the Spanner database."""
    creds, _ = google.auth.default()
    client = spanner.Client(project=PROJECT, credentials=creds)
    return client.instance(INSTANCE).database(DATABASE)


def find_ambiguous_groups(db: SpannerDatabase) -> list[list[str]]:
    """Find groups of company_ids that share the same LOWER(company_name).

    Returns:
        List of lists, where each inner list contains 2+ company_ids
        sharing the same normalized name.
    """
    sql = """
        SELECT LOWER(company_name) AS norm_name, ARRAY_AGG(company_id) AS ids
        FROM company
        GROUP BY LOWER(company_name)
        HAVING COUNT(*) >= 2
    """
    groups = []
    with db.snapshot() as snapshot:
        rows = list(snapshot.execute_sql(sql))
        for row in rows:
            norm_name = row[0]
            ids = list(row[1])
            logger.info(
                "Ambiguous name '%s' — %d companies: %s",
                norm_name, len(ids), ids,
            )
            groups.append(ids)
    return groups


def reset_all_flags(db: SpannerDatabase, dry_run: bool = False) -> int:
    """Set ambiguity_flag = false for ALL companies.

    Args:
        db: Spanner database handle.
        dry_run: If True, only report what would happen.

    Returns:
        Number of rows that had ambiguity_flag = true (before reset).
    """
    with db.snapshot() as snapshot:
        result = list(snapshot.execute_sql(
            "SELECT COUNT(*) FROM company WHERE ambiguity_flag = true"
        ))
        previously_flagged = result[0][0] if result else 0

    if dry_run:
        logger.info("[DRY RUN] Would reset %d previously flagged companies", previously_flagged)
        return previously_flagged

    def _reset(transaction):
        transaction.execute_update(
            "UPDATE company SET ambiguity_flag = false WHERE ambiguity_flag = true"
        )

    db.run_in_transaction(_reset)
    logger.info("Reset %d previously flagged companies to false", previously_flagged)
    return previously_flagged


def flag_ambiguous_companies(
    db: SpannerDatabase,
    groups: list[list[str]],
    dry_run: bool = False,
) -> int:
    """Set ambiguity_flag = true for all company_ids in ambiguous groups.

    Args:
        db: Spanner database handle.
        groups: List of company_id lists from find_ambiguous_groups().
        dry_run: If True, only report what would happen.

    Returns:
        Total number of companies flagged.
    """
    all_ids = []
    for group in groups:
        all_ids.extend(group)

    if not all_ids:
        logger.info("No ambiguous companies found — nothing to flag.")
        return 0

    if dry_run:
        logger.info("[DRY RUN] Would flag %d companies across %d groups", len(all_ids), len(groups))
        return len(all_ids)

    # Batch update — Spanner supports IN UNNEST for array params
    def _flag(transaction):
        transaction.execute_update(
            "UPDATE company SET ambiguity_flag = true "
            "WHERE company_id IN UNNEST(@ids)",
            params={"ids": all_ids},
            param_types={"ids": spanner.param_types.Array(spanner.param_types.STRING)},
        )

    db.run_in_transaction(_flag)
    logger.info("Flagged %d companies across %d ambiguous groups", len(all_ids), len(groups))
    return len(all_ids)


def main():
    parser = argparse.ArgumentParser(description="Populate company.ambiguity_flag")
    parser.add_argument("--dry-run", action="store_true", help="Report what would happen without writing")
    args = parser.parse_args()

    logger.info("Connecting to %s/%s/%s", PROJECT, INSTANCE, DATABASE)
    db = get_db()

    # Get total company count for logging
    with db.snapshot() as snapshot:
        total = list(snapshot.execute_sql("SELECT COUNT(*) FROM company"))[0][0]
    logger.info("Total companies in database: %d", total)

    # Step 1: Reset all flags
    logger.info("Step 1: Resetting all ambiguity flags...")
    reset_all_flags(db, dry_run=args.dry_run)

    # Step 2: Find ambiguous groups
    logger.info("Step 2: Finding companies with shared normalized names...")
    groups = find_ambiguous_groups(db)
    logger.info("Found %d ambiguous name groups", len(groups))

    # Step 3: Flag ambiguous companies
    logger.info("Step 3: Flagging ambiguous companies...")
    flagged = flag_ambiguous_companies(db, groups, dry_run=args.dry_run)

    # Summary
    logger.info("=" * 60)
    logger.info("SUMMARY: %d / %d companies flagged as ambiguous (%d groups)",
                flagged, total, len(groups))
    if args.dry_run:
        logger.info("[DRY RUN] No changes were written.")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
