"""
Step 9: Seed company_alias table with known aliases for ambiguous companies.

Inserts alias records into `company_alias` and corresponding edges into
`edge_is_alias_of` for well-known companies that have multiple names,
subsidiaries, or former names.

Idempotent: checks existence by alias_id before inserting. Safe to re-run.

Usage:
  python scripts/migrations/09_seed_company_aliases.py
  python scripts/migrations/09_seed_company_aliases.py --dry-run

Environment (falls back to defaults if not set):
  GCP_PROJECT_ID        default: gcp-poc-488614
  SPANNER_INSTANCE_ID   default: kg-dev-instance
  SPANNER_DATABASE_ID   default: kg_products_v4_dev
"""

import argparse
import hashlib
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

# ── Alias data ──────────────────────────────────────────────────────
# Format: company_name (must match company.company_name exactly)
#   → list of (alias_name, alias_type)
#
# alias_type: "subsidiary" | "former_name" | "abbreviation" | "trade_name"
#
# Only companies likely present in a 5,782-company expert network database
# are included. The script gracefully skips companies not found in the DB.

ALIAS_DATA: dict[str, list[tuple[str, str]]] = {
    "Shell": [
        ("Royal Dutch Shell", "former_name"),
        ("Shell plc", "trade_name"),
        ("Shell Energy", "subsidiary"),
    ],
    "Oracle": [
        ("Oracle Corporation", "trade_name"),
        ("Oracle Cloud", "subsidiary"),
    ],
    "Apple": [
        ("Apple Inc.", "trade_name"),
        ("Apple Inc", "trade_name"),
    ],
    "Amazon": [
        ("Amazon.com", "trade_name"),
        ("Amazon Web Services", "subsidiary"),
        ("AWS", "abbreviation"),
    ],
    "Google": [
        ("Alphabet", "trade_name"),
        ("Alphabet Inc.", "trade_name"),
        ("Google Cloud", "subsidiary"),
        ("Google LLC", "trade_name"),
    ],
    "Microsoft": [
        ("Microsoft Corporation", "trade_name"),
        ("Microsoft Azure", "subsidiary"),
        ("MSFT", "abbreviation"),
    ],
    "Meta": [
        ("Facebook", "former_name"),
        ("Meta Platforms", "trade_name"),
        ("Meta Platforms Inc.", "trade_name"),
    ],
    "IBM": [
        ("International Business Machines", "trade_name"),
        ("IBM Corporation", "trade_name"),
    ],
    "SAP": [
        ("SAP SE", "trade_name"),
        ("SAP America", "subsidiary"),
    ],
    "Accenture": [
        ("Accenture plc", "trade_name"),
        ("Accenture Federal Services", "subsidiary"),
    ],
    "Deloitte": [
        ("Deloitte Touche Tohmatsu", "trade_name"),
        ("Deloitte Consulting", "subsidiary"),
        ("Deloitte LLP", "trade_name"),
    ],
    "McKinsey": [
        ("McKinsey & Company", "trade_name"),
        ("McKinsey & Co.", "abbreviation"),
    ],
    "PwC": [
        ("PricewaterhouseCoopers", "trade_name"),
        ("PwC Advisory", "subsidiary"),
    ],
    "EY": [
        ("Ernst & Young", "trade_name"),
        ("Ernst & Young LLP", "trade_name"),
    ],
    "KPMG": [
        ("KPMG International", "trade_name"),
        ("KPMG LLP", "trade_name"),
    ],
    "Siemens": [
        ("Siemens AG", "trade_name"),
        ("Siemens Energy", "subsidiary"),
        ("Siemens Healthineers", "subsidiary"),
    ],
    "GE": [
        ("General Electric", "trade_name"),
        ("GE Healthcare", "subsidiary"),
        ("GE Aerospace", "subsidiary"),
    ],
    "HP": [
        ("Hewlett-Packard", "former_name"),
        ("HP Inc.", "trade_name"),
        ("Hewlett Packard Enterprise", "subsidiary"),
        ("HPE", "abbreviation"),
    ],
    "Salesforce": [
        ("Salesforce.com", "trade_name"),
        ("Salesforce Inc.", "trade_name"),
    ],
    "Cisco": [
        ("Cisco Systems", "trade_name"),
        ("Cisco Systems Inc.", "trade_name"),
    ],
}


def _make_alias_id(company_id: str, alias_name: str) -> str:
    """Generate a deterministic alias_id from company_id + alias_name.

    Args:
        company_id: The company's primary key.
        alias_name: The alias text.

    Returns:
        A deterministic STRING(256) suitable for alias_id PK.
    """
    h = hashlib.sha256(f"{company_id}:{alias_name}".encode()).hexdigest()[:16]
    return f"alias_{company_id}_{h}"


def get_db() -> SpannerDatabase:
    """Connect to the Spanner database."""
    creds, _ = google.auth.default()
    client = spanner.Client(project=PROJECT, credentials=creds)
    return client.instance(INSTANCE).database(DATABASE)


def lookup_company_id(db: SpannerDatabase, company_name: str) -> str | None:
    """Find a company_id by exact company_name match.

    Args:
        db: Spanner database handle.
        company_name: Exact company name to look up.

    Returns:
        company_id string, or None if not found.
    """
    with db.snapshot() as snapshot:
        rows = list(snapshot.execute_sql(
            "SELECT company_id FROM company WHERE name_raw = @name LIMIT 1",
            params={"name": company_name},
            param_types={"name": spanner.param_types.STRING},
        ))
    return rows[0][0] if rows else None


def get_existing_alias_ids(db: SpannerDatabase) -> set[str]:
    """Fetch all existing alias_ids for idempotency check.

    Returns:
        Set of existing alias_id strings.
    """
    with db.snapshot() as snapshot:
        rows = list(snapshot.execute_sql("SELECT alias_id FROM company_alias"))
    return {r[0] for r in rows}


def seed_aliases(db: SpannerDatabase, dry_run: bool = False) -> tuple[int, int]:
    """Insert alias records for known ambiguous companies.

    Args:
        db: Spanner database handle.
        dry_run: If True, only report what would happen.

    Returns:
        Tuple of (inserted_count, skipped_count).
    """
    existing_ids = get_existing_alias_ids(db)
    inserted = 0
    skipped = 0
    company_not_found = 0

    alias_rows = []   # (alias_id, company_id, alias_name, alias_type)
    edge_rows = []     # (alias_id, company_id)

    for company_name, aliases in ALIAS_DATA.items():
        company_id = lookup_company_id(db, company_name)
        if not company_id:
            logger.warning("Company '%s' not found in database — skipping aliases", company_name)
            company_not_found += 1
            continue

        for alias_name, alias_type in aliases:
            alias_id = _make_alias_id(company_id, alias_name)
            if alias_id in existing_ids:
                logger.debug("Alias '%s' for '%s' already exists — skipping", alias_name, company_name)
                skipped += 1
                continue

            alias_rows.append((alias_id, company_id, alias_name, alias_type))
            edge_rows.append((alias_id, company_id))
            inserted += 1
            logger.info("  + '%s' → '%s' (%s)", company_name, alias_name, alias_type)

    if dry_run:
        logger.info("[DRY RUN] Would insert %d aliases, skip %d existing, %d companies not found",
                    inserted, skipped, company_not_found)
        return inserted, skipped

    if alias_rows:
        def _insert(transaction):
            # Insert into company_alias
            transaction.insert(
                "company_alias",
                columns=["alias_id", "company_id", "alias_name", "alias_type"],
                values=alias_rows,
            )
            # Insert into edge_is_alias_of
            transaction.insert(
                "edge_is_alias_of",
                columns=["alias_id", "company_id"],
                values=edge_rows,
            )

        db.run_in_transaction(_insert)

    logger.info("Inserted %d aliases, skipped %d existing, %d companies not found",
                inserted, skipped, company_not_found)
    return inserted, skipped


def main():
    parser = argparse.ArgumentParser(description="Seed company_alias with known aliases")
    parser.add_argument("--dry-run", action="store_true", help="Report what would happen without writing")
    args = parser.parse_args()

    logger.info("Connecting to %s/%s/%s", PROJECT, INSTANCE, DATABASE)
    db = get_db()

    logger.info("Seeding aliases for %d companies...", len(ALIAS_DATA))
    inserted, skipped = seed_aliases(db, dry_run=args.dry_run)

    logger.info("=" * 60)
    logger.info("SUMMARY: %d inserted, %d skipped (already existed)", inserted, skipped)
    if args.dry_run:
        logger.info("[DRY RUN] No changes were written.")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
