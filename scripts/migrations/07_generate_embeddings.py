"""
Step 7: Generate text embeddings for the Knowledge Graph corpus.

Populates the two empty ARRAY<FLOAT32> columns added by 05_schema_changes.py
step 2.13:

  - knowledge_artifact.text_embedding          ← embeds knowledge_artifact.text
  - employment_record.responsibilities_embedding ← embeds employment_record.responsibilities

Uses Vertex AI Embedding API (text-embedding-005, 768 dims, English-specialized)
with task_type=RETRIEVAL_DOCUMENT.

Idempotent: only rows where the embedding column is NULL are processed, so
the script is safe to interrupt and re-run.

Usage:
  python scripts/migrations/07_generate_embeddings.py --dry-run
  python scripts/migrations/07_generate_embeddings.py
  python scripts/migrations/07_generate_embeddings.py --table knowledge_artifact
  python scripts/migrations/07_generate_embeddings.py --limit 100

Environment (falls back to .env values if not set):
  GCP_PROJECT_ID        default: gcp-poc-488614
  GCP_REGION            default: us-central1
  SPANNER_INSTANCE_ID   default: kg-dev-instance
  SPANNER_DATABASE_ID   default: kg_products_v4_dev
"""

import argparse
import os
import sys
import time
from dataclasses import dataclass

import google.auth
import vertexai
from google.api_core import exceptions as gax_exceptions
from google.cloud import spanner
from google.cloud.spanner_v1.database import Database as SpannerDatabase
from vertexai.language_models import TextEmbedding, TextEmbeddingInput, TextEmbeddingModel

# ── Config ──────────────────────────────────────────────────────────
PROJECT = os.getenv("GCP_PROJECT_ID", "gcp-poc-488614")
REGION = os.getenv("GCP_REGION", "us-central1")
INSTANCE = os.getenv("SPANNER_INSTANCE_ID", "kg-dev-instance")
DATABASE = os.getenv("SPANNER_DATABASE_ID", "kg_products_v4_dev")

EMBEDDING_MODEL = "text-embedding-005"
EMBEDDING_DIMS = 768
TASK_TYPE_INDEX = "RETRIEVAL_DOCUMENT"

# Spanner write batch — how many rows per db.batch().update() call.
# Matches BATCH_SIZE_DEFAULT from 06_copy_kg_v2_3.py.
SPANNER_WRITE_BATCH = 500

# NOTE on embedding-batch sizing (learned from first run on 2026-04-11):
# text-embedding-005 rejects requests whose *total* input-token count across
# all texts in one call exceeds ~20000 tokens (observed error:
# "input token count is 60134 but the model supports up to 20000"). This is
# a request-level limit, not a per-text limit. For short text columns like
# employment_record.responsibilities a batch of 32 × 8000 chars works fine
# (~2k tokens/text × 32 ≪ 20000 was a lucky average; it was actually right
# at the edge). For long transcripts / bios in knowledge_artifact.text,
# individual rows can exceed 15k tokens each, so batch=1 with a generous
# per-text cap is the only reliable strategy.
#
# The two knobs below live on TableSpec, not as module-level constants.

# Embedding-call retry policy for transient errors (ResourceExhausted,
# ServiceUnavailable, DeadlineExceeded). On persistent failure the whole
# batch is skipped and logged — those rows remain NULL and a re-run will
# pick them up.
MAX_RETRIES = 3
INITIAL_BACKOFF_SEC = 1.0


@dataclass(frozen=True)
class TableSpec:
    """Describes one table whose text column must be embedded.

    `embed_batch_size` is how many texts per Vertex AI request.
    `max_chars_per_text` is the hard char truncation applied in Python
    before sending (auto_truncate on the API side is a second safety net).

    The product embed_batch_size × (max_chars_per_text / 4) must stay
    under ~20000 tokens — see the comment at the top of the file.
    """

    table: str
    pk_col: str
    text_col: str
    emb_col: str
    embed_batch_size: int
    max_chars_per_text: int


TABLES: list[TableSpec] = [
    # Long transcripts / bios / Q&A. Some single rows exceed 15k tokens,
    # so we process one-at-a-time with a generous 60k-char cap
    # (~15k tokens worst-case, comfortably under the 20k request limit).
    TableSpec(
        table="knowledge_artifact",
        pk_col="artifact_id",
        text_col="text",
        emb_col="text_embedding",
        embed_batch_size=1,
        max_chars_per_text=60000,
    ),
    # Short free-form responsibilities (~a few hundred chars each).
    # Batch of 8 × 8000 chars = ~16k tokens worst-case, safely under 20k.
    # Reduced from 32 after the first run showed a transient near-edge
    # ResourceExhausted retry under the old 32-batch setting.
    TableSpec(
        table="employment_record",
        pk_col="employment_id",
        text_col="responsibilities",
        emb_col="responsibilities_embedding",
        embed_batch_size=8,
        max_chars_per_text=8000,
    ),
]


# ── Spanner helpers ─────────────────────────────────────────────────

def get_db() -> SpannerDatabase:
    """Return a Spanner Database handle using ADC."""
    creds, _ = google.auth.default()
    client = spanner.Client(project=PROJECT, credentials=creds)
    return client.instance(INSTANCE).database(DATABASE)


def fetch_eligible_rows(
    db: SpannerDatabase,
    spec: TableSpec,
    limit: int | None,
) -> list[tuple[str, str]]:
    """Return (pk, text) for all rows that still need an embedding."""
    sql = f"""
        SELECT {spec.pk_col}, {spec.text_col}
        FROM {spec.table}
        WHERE {spec.text_col} IS NOT NULL
          AND {spec.text_col} != ''
          AND {spec.emb_col} IS NULL
    """
    if limit is not None:
        sql += f"\n        LIMIT {int(limit)}"

    with db.snapshot() as snap:
        results = snap.execute_sql(sql)
        return [(row[0], row[1]) for row in results]


def table_stats(db: SpannerDatabase, spec: TableSpec) -> dict[str, int]:
    """Return row counts: total / eligible / embedded / still_missing."""
    sql = f"""
        SELECT
          COUNT(*) AS total,
          COUNTIF({spec.text_col} IS NOT NULL AND {spec.text_col} != '') AS eligible,
          COUNTIF({spec.emb_col} IS NOT NULL) AS embedded,
          COUNTIF(
            {spec.text_col} IS NOT NULL
            AND {spec.text_col} != ''
            AND {spec.emb_col} IS NULL
          ) AS still_missing
        FROM {spec.table}
    """
    with db.snapshot() as snap:
        row = next(iter(snap.execute_sql(sql)))
    return {
        "total": int(row[0]),
        "eligible": int(row[1]),
        "embedded": int(row[2]),
        "still_missing": int(row[3]),
    }


# ── Embedding helpers ───────────────────────────────────────────────

def embed_with_retry(
    model: TextEmbeddingModel,
    texts: list[str],
) -> list[TextEmbedding] | None:
    """Call the embedding API with retry on transient errors.

    Returns None if all retries failed — caller decides what to do (we
    log and skip the batch so the next run picks those rows up).
    """
    inputs = [TextEmbeddingInput(text=t, task_type=TASK_TYPE_INDEX) for t in texts]

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return model.get_embeddings(inputs, auto_truncate=True)
        except (
            gax_exceptions.ResourceExhausted,
            gax_exceptions.ServiceUnavailable,
            gax_exceptions.DeadlineExceeded,
            gax_exceptions.InternalServerError,
        ) as e:
            backoff = INITIAL_BACKOFF_SEC * (2 ** (attempt - 1))
            print(
                f"    ⚠️  transient error ({type(e).__name__}): retrying in {backoff:.1f}s "
                f"(attempt {attempt}/{MAX_RETRIES})"
            )
            if attempt == MAX_RETRIES:
                print(f"    ❌ giving up on batch: {e}")
                return None
            time.sleep(backoff)
        except Exception as e:
            # Non-retryable (e.g. InvalidArgument from a single bad text).
            print(f"    ❌ non-retryable error: {type(e).__name__}: {e}")
            return None
    return None


def pre_flight_check(model: TextEmbeddingModel) -> None:
    """Single-call sanity check — proves auth/region/model availability.

    Raises on failure so the script aborts early instead of burning time
    on a loop that will fail every batch.
    """
    print(f"  Pre-flight: calling {EMBEDDING_MODEL} with a ping sample...")
    result = model.get_embeddings(
        [TextEmbeddingInput(text="ping", task_type=TASK_TYPE_INDEX)],
        auto_truncate=True,
    )
    if not result or len(result) != 1:
        raise RuntimeError(
            f"Pre-flight returned {len(result) if result else 0} embeddings, expected 1"
        )
    dims = len(result[0].values)
    if dims != EMBEDDING_DIMS:
        raise RuntimeError(
            f"Pre-flight returned vector of {dims} dims, expected {EMBEDDING_DIMS}"
        )
    print(f"  ✅ Pre-flight OK — received 1 vector of {dims} floats")


# ── Populate loop ───────────────────────────────────────────────────

def populate(
    db: SpannerDatabase,
    model: TextEmbeddingModel,
    spec: TableSpec,
    *,
    row_limit: int | None,
) -> tuple[int, int]:
    """Embed and write rows for one table. Returns (written, failed)."""
    print(f"\n▶ {spec.table}.{spec.text_col} → {spec.emb_col}")

    rows = fetch_eligible_rows(db, spec, limit=row_limit)
    eligible = len(rows)
    if eligible == 0:
        print(f"  {spec.table}: nothing to do (0 eligible rows)")
        return 0, 0

    print(
        f"  {spec.table}: {eligible:,} eligible rows "
        f"(batch={spec.embed_batch_size}, cap={spec.max_chars_per_text:,} chars)"
    )

    # Accumulate (pk, embedding) pairs across embedding-batches and flush
    # to Spanner in larger chunks to amortize write overhead.
    pending_writes: list[tuple[str, list[float]]] = []
    total_written = 0
    total_failed = 0
    start = time.time()

    for i in range(0, eligible, spec.embed_batch_size):
        batch = rows[i : i + spec.embed_batch_size]
        pks = [pk for pk, _ in batch]
        texts = [(text or "")[: spec.max_chars_per_text] for _, text in batch]

        embeddings = embed_with_retry(model, texts)
        if embeddings is None:
            total_failed += len(batch)
            print(f"    skipped {len(batch)} rows, first pk={pks[0]!r}")
            continue

        for pk, emb in zip(pks, embeddings):
            pending_writes.append((pk, list(emb.values)))

        if len(pending_writes) >= SPANNER_WRITE_BATCH:
            total_written += flush_writes(db, spec, pending_writes)
            pending_writes = []
            elapsed = time.time() - start
            rate = total_written / elapsed if elapsed > 0 else 0.0
            print(
                f"    progress: {total_written:,}/{eligible:,} "
                f"({rate:.0f} rows/s, {elapsed:.0f}s elapsed)"
            )

    if pending_writes:
        total_written += flush_writes(db, spec, pending_writes)

    elapsed = time.time() - start
    print(
        f"  ✅ {spec.table}: wrote {total_written:,} embeddings "
        f"({total_failed} failed) in {elapsed:.1f}s"
    )
    return total_written, total_failed


def flush_writes(
    db: SpannerDatabase,
    spec: TableSpec,
    pending: list[tuple[str, list[float]]],
) -> int:
    """Write accumulated (pk, vector) pairs to Spanner in sub-batches."""
    if not pending:
        return 0
    written = 0
    for i in range(0, len(pending), SPANNER_WRITE_BATCH):
        chunk = pending[i : i + SPANNER_WRITE_BATCH]
        values = [(pk, vec) for pk, vec in chunk]
        with db.batch() as b:
            b.update(
                table=spec.table,
                columns=[spec.pk_col, spec.emb_col],
                values=values,
            )
        written += len(chunk)
    return written


# ── Verification ────────────────────────────────────────────────────

def print_stats(db: SpannerDatabase, spec: TableSpec, header: str) -> dict[str, int]:
    stats = table_stats(db, spec)
    print(f"  {header} {spec.table}:")
    print(f"    total         = {stats['total']:,}")
    print(f"    eligible      = {stats['eligible']:,}")
    print(f"    embedded      = {stats['embedded']:,}")
    print(f"    still_missing = {stats['still_missing']:,}")
    return stats


# ── Main ────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Generate Vertex AI text embeddings and write them to the "
            "Spanner knowledge graph. Idempotent — re-run to resume after "
            "interruption."
        ),
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Do not write anything. Prints per-table eligible counts and "
            "runs one pre-flight embedding call to verify auth and region."
        ),
    )
    p.add_argument(
        "--table",
        choices=[t.table for t in TABLES],
        default=None,
        help="Process only one table (default: all tables in TABLES).",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Process at most N rows per table (for smoke-testing the full "
            "read→embed→write path on a small subset). Default: all rows."
        ),
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()

    print(f"Target: {PROJECT}/{INSTANCE}/{DATABASE}")
    print(f"Vertex AI: {REGION} / model={EMBEDDING_MODEL} / task={TASK_TYPE_INDEX}")
    print(f"Mode: {'DRY-RUN (no writes)' if args.dry_run else 'LIVE'}")
    if args.table:
        print(f"Table filter: {args.table}")
    if args.limit is not None:
        print(f"Row limit per table: {args.limit}")

    specs = [s for s in TABLES if args.table is None or s.table == args.table]

    # ── Connect to Spanner ───────────────────────────────────────────
    print("\n[1/4] Connecting to Spanner...")
    db = get_db()
    print("  ✅ Spanner connected")

    # ── Before-stats ─────────────────────────────────────────────────
    print("\n[2/4] Current state (BEFORE):")
    before: dict[str, dict[str, int]] = {}
    for spec in specs:
        before[spec.table] = print_stats(db, spec, header="•")

    # ── Vertex AI init + pre-flight ──────────────────────────────────
    print("\n[3/4] Initializing Vertex AI + pre-flight check...")
    vertexai.init(project=PROJECT, location=REGION)
    model = TextEmbeddingModel.from_pretrained(EMBEDDING_MODEL)
    pre_flight_check(model)

    if args.dry_run:
        print("\n✋ DRY-RUN: skipping the embed/write loop.")
        print("   To run for real: remove --dry-run.")
        return 0

    # ── Populate ─────────────────────────────────────────────────────
    print("\n[4/4] Embedding and writing...")
    grand_written = 0
    grand_failed = 0
    for spec in specs:
        written, failed = populate(db, model, spec, row_limit=args.limit)
        grand_written += written
        grand_failed += failed

    # ── After-stats ──────────────────────────────────────────────────
    print("\nFinal state (AFTER):")
    exit_code = 0
    for spec in specs:
        after = print_stats(db, spec, header="•")
        # Acceptance criterion: nothing eligible left without an embedding.
        if args.limit is None and after["still_missing"] != 0:
            print(
                f"    ⚠️  {spec.table}: {after['still_missing']} rows still missing "
                f"an embedding — re-run to retry."
            )
            exit_code = 1

    print(
        f"\nSummary: wrote {grand_written:,} embeddings total, "
        f"{grand_failed} failures"
    )
    if exit_code == 0:
        print("✅ Done — all eligible rows have embeddings.")
    else:
        print("⚠️  Done with gaps — see warnings above.")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
