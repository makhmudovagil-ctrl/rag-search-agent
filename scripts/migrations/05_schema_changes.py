"""
Part 2: Apply schema changes from planning.md (sections 2.1–2.13).

Organized by priority:
  P0 — Data Cleanup + Coverage Metadata (2.1–2.4)
  P1 — Entity Disambiguation + Routing Signals (2.5–2.9)
  P2 — Missing Keyword Edges + Confidence + Embeddings (2.10–2.13)

Each step is idempotent — safe to re-run.
"""

from google.cloud import spanner

# ── Config ──────────────────────────────────────────────────────────
PROJECT = "gcp-poc-488614"
INSTANCE = "kg-dev-instance"
DATABASE = "kg_products_v4_dev"


def run_dml(db, label: str, statements: list[str]):
    """Run DML statements in a single transaction."""
    def _work(transaction):
        for stmt in statements:
            row_count = transaction.execute_update(stmt)
            print(f"    {row_count} rows affected")

    print(f"  {label}...")
    db.run_in_transaction(_work)


def run_ddl(db, label: str, statements: list[str]):
    """Run DDL statements (ALTER TABLE, CREATE TABLE, CREATE INDEX)."""
    print(f"  {label}...")
    operation = db.update_ddl(statements)
    operation.result(timeout=300)
    print(f"    ✅ DDL applied ({len(statements)} statement(s))")


# ═══════════════════════════════════════════════════════════════════
# P0 — Data Cleanup
# ═══════════════════════════════════════════════════════════════════

def step_2_1_normalize_supply_chain_position(db):
    """2.1: Normalize supply_chain_position on edge_involved_with.

    Current state (23 variants): buyer/Buyer/Buyer/User/former buyer/
    previous customer/user/evaluator/etc.
    Target: 10 clean lowercase values.
    """
    print("\n[2.1] Normalize supply_chain_position")

    # Step 1: lowercase + trim everything
    run_dml(db, "Lowercase + trim", [
        """UPDATE edge_involved_with
           SET supply_chain_position = LOWER(TRIM(supply_chain_position))
           WHERE supply_chain_position IS NOT NULL
             AND supply_chain_position != ''
             AND supply_chain_position != LOWER(TRIM(supply_chain_position))""",
    ])

    # Step 2: merge compound/legacy values
    run_dml(db, "Merge buyer/user → buyer", [
        """UPDATE edge_involved_with
           SET supply_chain_position = 'buyer'
           WHERE supply_chain_position IN ('buyer/user', 'former buyer', 'previous customer')""",
    ])

    run_dml(db, "Merge user/evaluator → user", [
        """UPDATE edge_involved_with
           SET supply_chain_position = 'user'
           WHERE supply_chain_position = 'user/evaluator'""",
    ])

    # Step 3: empty string → none
    run_dml(db, "Empty string → none", [
        """UPDATE edge_involved_with
           SET supply_chain_position = 'none'
           WHERE supply_chain_position = ''""",
    ])

    # Verify
    _verify_supply_chain(db)


def _verify_supply_chain(db):
    """Print current distinct values after cleanup."""
    with db.snapshot() as snap:
        results = snap.execute_sql("""
            SELECT supply_chain_position, COUNT(*) as cnt
            FROM edge_involved_with
            WHERE supply_chain_position IS NOT NULL
            GROUP BY supply_chain_position
            ORDER BY cnt DESC
        """)
        print("    Final values:")
        for row in results:
            print(f"      {row[0]:20s} → {row[1]}")


def step_2_2_deduplicate_role_function(db):
    """2.2: Deduplicate role.function — merge 'Human Resources' → 'HR'."""
    print("\n[2.2] Deduplicate role.function")

    run_dml(db, "Human Resources → HR", [
        """UPDATE role SET function = 'HR'
           WHERE function = 'Human Resources'""",
    ])

    # Verify
    with db.snapshot() as snap:
        results = snap.execute_sql("""
            SELECT function, COUNT(*) as cnt
            FROM role
            GROUP BY function
            ORDER BY cnt DESC
        """)
        print("    Final values:")
        for row in results:
            print(f"      {str(row[0]):20s} → {row[1]}")


# ═══════════════════════════════════════════════════════════════════
# P0 — Coverage Metadata
# ═══════════════════════════════════════════════════════════════════

def step_2_3_add_expert_count(db):
    """2.3: Add expert_count to company + populate."""
    print("\n[2.3] Add expert_count to company")

    run_ddl(db, "ALTER TABLE", [
        "ALTER TABLE company ADD COLUMN expert_count INT64",
    ])

    run_dml(db, "Populate expert_count", [
        """UPDATE company c SET c.expert_count = (
            SELECT COUNT(DISTINCT er.expert_id)
            FROM edge_at_company eac
            JOIN employment_record er ON eac.employment_id = er.employment_id
            WHERE eac.company_id = c.company_id
        ) WHERE TRUE""",
    ])

    # Verify: top companies by expert count
    with db.snapshot() as snap:
        results = snap.execute_sql("""
            SELECT company_name, expert_count
            FROM company
            WHERE expert_count > 0
            ORDER BY expert_count DESC
            LIMIT 5
        """)
        print("    Top 5 companies by expert_count:")
        for row in results:
            print(f"      {row[0]:40s} → {row[1]}")


def step_2_4_add_artifact_count(db):
    """2.4: Add artifact_count to product + populate."""
    print("\n[2.4] Add artifact_count to product")

    run_ddl(db, "ALTER TABLE", [
        "ALTER TABLE product ADD COLUMN artifact_count INT64",
    ])

    run_dml(db, "Populate artifact_count", [
        """UPDATE product p SET p.artifact_count = (
            SELECT COUNT(*)
            FROM edge_involved_with eiw
            WHERE eiw.product_id = p.product_id
        ) WHERE TRUE""",
    ])

    # Verify
    with db.snapshot() as snap:
        results = snap.execute_sql("""
            SELECT product_name, artifact_count
            FROM product
            WHERE artifact_count > 0
            ORDER BY artifact_count DESC
            LIMIT 5
        """)
        print("    Top 5 products by artifact_count:")
        for row in results:
            print(f"      {row[0]:40s} → {row[1]}")


# ═══════════════════════════════════════════════════════════════════
# P1 — Entity Disambiguation
# ═══════════════════════════════════════════════════════════════════

def step_2_5_add_ambiguity_flag(db):
    """2.5: Add ambiguity_flag to company."""
    print("\n[2.5] Add ambiguity_flag to company")

    run_ddl(db, "ALTER TABLE", [
        "ALTER TABLE company ADD COLUMN ambiguity_flag BOOL DEFAULT (false)",
    ])


def step_2_6_create_company_alias(db):
    """2.6: Create company_alias table + indexes."""
    print("\n[2.6] Create company_alias table")

    run_ddl(db, "CREATE TABLE + indexes", [
        """CREATE TABLE company_alias (
            alias_id STRING(256) NOT NULL,
            company_id STRING(256) NOT NULL,
            alias_name STRING(512) NOT NULL,
            alias_type STRING(32),
        ) PRIMARY KEY(alias_id)""",
        "CREATE INDEX idx_alias_name ON company_alias(alias_name)",
        "CREATE INDEX idx_alias_company ON company_alias(company_id)",
    ])


def step_2_6b_create_edge_is_alias_of(db):
    """2.6b: Create edge_is_alias_of table for Property Graph.

    Spanner Property Graph doesn't allow a table to appear as both
    node and edge. company_alias is a node; this is the edge table.
    When inserting alias data, insert into BOTH company_alias and edge_is_alias_of.
    """
    print("\n[2.6b] Create edge_is_alias_of table")

    run_ddl(db, "CREATE TABLE", [
        """CREATE TABLE edge_is_alias_of (
            alias_id STRING(256) NOT NULL,
            company_id STRING(256) NOT NULL,
        ) PRIMARY KEY(alias_id, company_id)""",
    ])


# ═══════════════════════════════════════════════════════════════════
# P1 — Routing Signals
# ═══════════════════════════════════════════════════════════════════

def step_2_8_add_expected_resolution(db):
    """2.8: Add expected_resolution to keyword edge tables."""
    print("\n[2.8] Add expected_resolution to keyword edges")

    run_ddl(db, "ALTER TABLE (2 tables)", [
        "ALTER TABLE edge_maps_to_product ADD COLUMN expected_resolution STRING(16)",
        "ALTER TABLE edge_maps_to_industry ADD COLUMN expected_resolution STRING(16)",
    ])


def step_2_9_add_function_confidence(db):
    """2.9: Add function_confidence to edge_has_role."""
    print("\n[2.9] Add function_confidence to edge_has_role")

    run_ddl(db, "ALTER TABLE", [
        "ALTER TABLE edge_has_role ADD COLUMN function_confidence FLOAT64",
    ])


# ═══════════════════════════════════════════════════════════════════
# P1 — Update Property Graph (2.7)
# ═══════════════════════════════════════════════════════════════════

def step_2_7_update_property_graph(db):
    """2.7: Update Property Graph to include new columns + company_alias edge.

    Must run AFTER 2.3-2.6, 2.8-2.9 since the graph references
    new columns and the company_alias table.
    """
    print("\n[2.7] Update Property Graph (add new columns + company_alias edge)")

    # Full graph redefinition with:
    # - company: +expert_count, +ambiguity_flag
    # - product: +artifact_count
    # - edge_maps_to_product: +expected_resolution
    # - edge_maps_to_industry: +expected_resolution
    # - edge_has_role: +function_confidence
    # - NEW: company_alias node + IS_ALIAS_OF edge
    ddl = """CREATE OR REPLACE PROPERTY GRAPH kg_graph
  NODE TABLES(
    angle
      KEY(angle_id)
      LABEL Angle PROPERTIES(
        angle_bio, angle_id, customers_of_providers, name, priority,
        scope, tenure, type),
    company
      KEY(company_id)
      LABEL Company PROPERTIES(
        company_id, company_name, company_type, description, founded,
        linkedin_url, market_position, name_normalised, size, website,
        expert_count, ambiguity_flag),
    company_alias
      KEY(alias_id)
      LABEL CompanyAlias PROPERTIES(
        alias_id, company_id, alias_name, alias_type),
    competitive_set
      KEY(competitive_set_id)
      LABEL CompetitiveSet PROPERTIES(
        competitive_set_id, customer_segment, definition, name,
        perspective, project_id, use_cases, validated_in_artifact_id),
    employment_record
      KEY(employment_id)
      LABEL EmploymentRecord PROPERTIES(
        company_id, employment_id, end_date, end_month, end_year,
        expert_id, geo, is_current, jobtitle_raw, position,
        responsibilities, start_date, start_month, start_year),
    expert
      KEY(expert_id)
      LABEL Expert PROPERTIES(
        expert_id, expert_name, is_active, skills),
    geography
      KEY(geography_id)
      LABEL Geography PROPERTIES(
        geography_id, level, name, parent_id),
    industry
      KEY(industry_id)
      LABEL Industry PROPERTIES(
        description, industry_id, name),
    keyword
      KEY(keyword_id)
      LABEL Keyword PROPERTIES(
        keyword, keyword_id, source),
    knowledge_artifact
      KEY(artifact_id)
      LABEL KnowledgeArtifact PROPERTIES(
        artifact_id, artifact_type, call_id, date, expert_id, lens,
        lens_rationale, project_id, relevant_employment_id, text),
    product
      KEY(product_id)
      LABEL Product PROPERTIES(
        product_id, product_name, source, source_artifact_id,
        vendor_company_id, vendor_name, artifact_count),
    product_category
      KEY(product_category_id)
      LABEL ProductCategory PROPERTIES(
        category_parent, name, product_category_id),
    project
      KEY(project_id)
      LABEL Project PROPERTIES(
        brief_dimension, brief_driver, brief_lens, brief_object,
        client_company_id, client_company_name, created_at, project_id,
        project_name, project_type_raw, software_focus),
    role
      KEY(role_id)
      LABEL Role PROPERTIES(
        `function` AS `function`, org_scope, role_id, role_name, seniority),
    subindustry
      KEY(subindustry_id)
      LABEL SubIndustry PROPERTIES(
        definition, industry_ids, name, subindustry_id)
  )
  EDGE TABLES(
    edge_angle_targets_company
      KEY(angle_id, company_id)
      SOURCE KEY(angle_id) REFERENCES angle(angle_id)
      DESTINATION KEY(company_id) REFERENCES company(company_id)
      LABEL ANGLE_TARGETS_COMPANY PROPERTIES(angle_id, company_id),
    edge_angle_targets_industry
      KEY(angle_id, industry_id)
      SOURCE KEY(angle_id) REFERENCES angle(angle_id)
      DESTINATION KEY(industry_id) REFERENCES industry(industry_id)
      LABEL ANGLE_TARGETS_INDUSTRY PROPERTIES(angle_id, industry_id),
    edge_angle_targets_product
      KEY(angle_id, product_id)
      SOURCE KEY(angle_id) REFERENCES angle(angle_id)
      DESTINATION KEY(product_id) REFERENCES product(product_id)
      LABEL ANGLE_TARGETS_PRODUCT PROPERTIES(angle_id, product_id, source),
    edge_angle_targets_subindustry
      KEY(angle_id, subindustry_id)
      SOURCE KEY(angle_id) REFERENCES angle(angle_id)
      DESTINATION KEY(subindustry_id) REFERENCES subindustry(subindustry_id)
      LABEL ANGLE_TARGETS_SUBINDUSTRY PROPERTIES(angle_id, subindustry_id),
    edge_at_company
      KEY(employment_id)
      SOURCE KEY(employment_id) REFERENCES employment_record(employment_id)
      DESTINATION KEY(company_id) REFERENCES company(company_id)
      LABEL AT_COMPANY PROPERTIES(company_id, employment_id),
    edge_based_in_country
      KEY(employment_id, geography_id)
      SOURCE KEY(employment_id) REFERENCES employment_record(employment_id)
      DESTINATION KEY(geography_id) REFERENCES geography(geography_id)
      LABEL BASED_IN_COUNTRY PROPERTIES(employment_id, geography_id),
    edge_belongs_to_category
      KEY(product_id, product_category_id)
      SOURCE KEY(product_id) REFERENCES product(product_id)
      DESTINATION KEY(product_category_id) REFERENCES product_category(product_category_id)
      LABEL BELONGS_TO_CATEGORY PROPERTIES(product_category_id, product_id),
    edge_competitive_set_for
      KEY(competitive_set_id, project_id)
      SOURCE KEY(competitive_set_id) REFERENCES competitive_set(competitive_set_id)
      DESTINATION KEY(project_id) REFERENCES project(project_id)
      LABEL COMPETITIVE_SET_FOR PROPERTIES(competitive_set_id, project_id),
    edge_competitor_of
      KEY(company_id_a, company_id_b)
      SOURCE KEY(company_id_a) REFERENCES company(company_id)
      DESTINATION KEY(company_id_b) REFERENCES company(company_id)
      LABEL COMPETITOR_OF PROPERTIES(company_id_a, company_id_b, source),
    edge_covers
      KEY(employment_id, geography_id)
      SOURCE KEY(employment_id) REFERENCES employment_record(employment_id)
      DESTINATION KEY(geography_id) REFERENCES geography(geography_id)
      LABEL COVERS PROPERTIES(employment_id, geography_id),
    edge_customer_of
      KEY(from_company_id, to_company_id)
      SOURCE KEY(from_company_id) REFERENCES company(company_id)
      DESTINATION KEY(to_company_id) REFERENCES company(company_id)
      LABEL CUSTOMER_OF PROPERTIES(
        end_date, from_company_id, status, to_company_id,
        validated_in_artifact_id),
    edge_features
      KEY(artifact_id, expert_id)
      SOURCE KEY(artifact_id) REFERENCES knowledge_artifact(artifact_id)
      DESTINATION KEY(expert_id) REFERENCES expert(expert_id)
      LABEL FEATURES PROPERTIES(artifact_id, expert_id),
    edge_for_angle
      KEY(artifact_id, angle_id)
      SOURCE KEY(artifact_id) REFERENCES knowledge_artifact(artifact_id)
      DESTINATION KEY(angle_id) REFERENCES angle(angle_id)
      LABEL FOR_ANGLE PROPERTIES(angle_id, artifact_id),
    edge_for_project
      KEY(artifact_id, project_id)
      SOURCE KEY(artifact_id) REFERENCES knowledge_artifact(artifact_id)
      DESTINATION KEY(project_id) REFERENCES project(project_id)
      LABEL PRODUCED_FOR PROPERTIES(artifact_id, project_id),
    edge_has_employment
      KEY(expert_id, employment_id)
      SOURCE KEY(expert_id) REFERENCES expert(expert_id)
      DESTINATION KEY(employment_id) REFERENCES employment_record(employment_id)
      LABEL HAS_EMPLOYMENT PROPERTIES(employment_id, expert_id),
    edge_has_role
      KEY(employment_id, role_id)
      SOURCE KEY(employment_id) REFERENCES employment_record(employment_id)
      DESTINATION KEY(role_id) REFERENCES role(role_id)
      LABEL HAS_ROLE PROPERTIES(employment_id, role_id, function_confidence),
    edge_in_competitive_set
      KEY(product_id, competitive_set_id)
      SOURCE KEY(product_id) REFERENCES product(product_id)
      DESTINATION KEY(competitive_set_id) REFERENCES competitive_set(competitive_set_id)
      LABEL IN_COMPETITIVE_SET PROPERTIES(competitive_set_id, product_id),
    edge_in_industry
      KEY(company_id, industry_id)
      SOURCE KEY(company_id) REFERENCES company(company_id)
      DESTINATION KEY(industry_id) REFERENCES industry(industry_id)
      LABEL IN_INDUSTRY PROPERTIES(company_id, industry_id),
    edge_in_subindustry
      KEY(company_id, subindustry_id)
      SOURCE KEY(company_id) REFERENCES company(company_id)
      DESTINATION KEY(subindustry_id) REFERENCES subindustry(subindustry_id)
      LABEL IN_SUB_INDUSTRY PROPERTIES(company_id, subindustry_id),
    edge_involved_with
      KEY(employment_id, product_id, source_artifact_id)
      SOURCE KEY(employment_id) REFERENCES employment_record(employment_id)
      DESTINATION KEY(product_id) REFERENCES product(product_id)
      LABEL INVOLVED_WITH PROPERTIES(
        attachment_method, employment_id, end_date, is_evaluator,
        is_key_decision_maker, is_user, notes, product_id,
        source_artifact_id, start_date, status, supply_chain_position,
        validation_source),
    edge_maps_to_industry
      KEY(keyword_id, industry_id)
      SOURCE KEY(keyword_id) REFERENCES keyword(keyword_id)
      DESTINATION KEY(industry_id) REFERENCES industry(industry_id)
      LABEL MAPS_TO_INDUSTRY PROPERTIES(industry_id, keyword_id, expected_resolution),
    edge_maps_to_product
      KEY(keyword_id, product_id)
      SOURCE KEY(keyword_id) REFERENCES keyword(keyword_id)
      DESTINATION KEY(product_id) REFERENCES product(product_id)
      LABEL MAPS_TO_PRODUCT PROPERTIES(keyword_id, product_id, expected_resolution),
    edge_mentioned_in
      KEY(keyword_id, artifact_id)
      SOURCE KEY(keyword_id) REFERENCES keyword(keyword_id)
      DESTINATION KEY(artifact_id) REFERENCES knowledge_artifact(artifact_id)
      LABEL MENTIONED_IN PROPERTIES(artifact_id, keyword_id),
    edge_produced_by
      KEY(product_id, company_id)
      SOURCE KEY(product_id) REFERENCES product(product_id)
      DESTINATION KEY(company_id) REFERENCES company(company_id)
      LABEL PRODUCED_BY PROPERTIES(company_id, product_id),
    edge_project_has_angle
      KEY(project_id, angle_id)
      SOURCE KEY(project_id) REFERENCES project(project_id)
      DESTINATION KEY(angle_id) REFERENCES angle(angle_id)
      LABEL PROJECT_HAS_ANGLE PROPERTIES(angle_id, project_id),
    edge_relevant_employment
      KEY(artifact_id, employment_id)
      SOURCE KEY(artifact_id) REFERENCES knowledge_artifact(artifact_id)
      DESTINATION KEY(employment_id) REFERENCES employment_record(employment_id)
      LABEL RELEVANT_EMPLOYMENT PROPERTIES(artifact_id, context_type, employment_id),
    edge_subindustry_of
      KEY(subindustry_id, industry_id)
      SOURCE KEY(subindustry_id) REFERENCES subindustry(subindustry_id)
      DESTINATION KEY(industry_id) REFERENCES industry(industry_id)
      LABEL SUB_INDUSTRY_OF PROPERTIES(industry_id, subindustry_id),
    edge_supplier_of
      KEY(from_company_id, to_company_id)
      SOURCE KEY(from_company_id) REFERENCES company(company_id)
      DESTINATION KEY(to_company_id) REFERENCES company(company_id)
      LABEL SUPPLIER_OF PROPERTIES(
        end_date, from_company_id, status, to_company_id,
        validated_in_artifact_id),
    edge_is_alias_of
      KEY(alias_id, company_id)
      SOURCE KEY(alias_id) REFERENCES company_alias(alias_id)
      DESTINATION KEY(company_id) REFERENCES company(company_id)
      LABEL IS_ALIAS_OF PROPERTIES(alias_id, company_id),
    edge_selected_for
      KEY(expert_id, project_id)
      SOURCE KEY(expert_id) REFERENCES expert(expert_id)
      DESTINATION KEY(project_id) REFERENCES project(project_id)
      LABEL SELECTED_FOR PROPERTIES(
        expert_id, project_id, qualification_id, angle_id,
        selection_status, relevant_employment_id, selected_at)
  )"""

    run_ddl(db, "CREATE OR REPLACE PROPERTY GRAPH", [ddl])


# ═══════════════════════════════════════════════════════════════════
# P2 — Missing Keyword Edges
# ═══════════════════════════════════════════════════════════════════

def step_2_10_create_edge_maps_to_function(db):
    """2.10: Create edge_maps_to_function table."""
    print("\n[2.10] Create edge_maps_to_function")

    run_ddl(db, "CREATE TABLE", [
        """CREATE TABLE edge_maps_to_function (
            keyword_id STRING(256) NOT NULL,
            role_function STRING(64) NOT NULL,
            confidence FLOAT64,
        ) PRIMARY KEY(keyword_id, role_function)""",
    ])


def step_2_11_create_edge_maps_to_product_category(db):
    """2.11: Create edge_maps_to_product_category table."""
    print("\n[2.11] Create edge_maps_to_product_category")

    run_ddl(db, "CREATE TABLE", [
        """CREATE TABLE edge_maps_to_product_category (
            keyword_id STRING(256) NOT NULL,
            product_category_id STRING(256) NOT NULL,
            confidence FLOAT64,
        ) PRIMARY KEY(keyword_id, product_category_id)""",
    ])


# ═══════════════════════════════════════════════════════════════════
# P2 — Confidence & Selection Signals
# ═══════════════════════════════════════════════════════════════════

def step_2_12_add_selection_count(db):
    """2.12: Add selection_count to keyword + populate."""
    print("\n[2.12] Add selection_count to keyword")

    run_ddl(db, "ALTER TABLE", [
        "ALTER TABLE keyword ADD COLUMN selection_count INT64 DEFAULT (0)",
    ])

    # Populate: count how many times each keyword's associated experts
    # were selected. Chain: keyword → edge_mentioned_in → knowledge_artifact
    # → expert (via expert_id) → edge_selected_for
    run_dml(db, "Populate selection_count", [
        """UPDATE keyword k SET k.selection_count = (
            SELECT COUNT(DISTINCT esf.project_id)
            FROM edge_mentioned_in emi
            JOIN knowledge_artifact ka ON emi.artifact_id = ka.artifact_id
            JOIN edge_selected_for esf ON ka.expert_id = esf.expert_id
            WHERE emi.keyword_id = k.keyword_id
              AND esf.selection_status IN ('Completed', 'Outreached')
        ) WHERE TRUE""",
    ])

    # Verify
    with db.snapshot() as snap:
        results = snap.execute_sql("""
            SELECT k.keyword_id, k.selection_count
            FROM keyword k
            WHERE k.selection_count > 0
            ORDER BY k.selection_count DESC
            LIMIT 5
        """)
        print("    Top 5 keywords by selection_count:")
        for row in results:
            print(f"      {str(row[0]):40s} → {row[1]}")


# ═══════════════════════════════════════════════════════════════════
# P2 — Graph-Native Embeddings (columns only — population is separate)
# ═══════════════════════════════════════════════════════════════════

def step_2_13_add_embedding_columns(db):
    """2.13: Add vector embedding columns (empty — populated later via Vertex AI)."""
    print("\n[2.13] Add embedding columns")

    # NOTE: VECTOR type requires Spanner to support it in this edition.
    # If it fails, we skip — embeddings are P2 and can be added later.
    try:
        run_ddl(db, "ALTER TABLE (vector columns)", [
            "ALTER TABLE knowledge_artifact ADD COLUMN text_embedding ARRAY<FLOAT32>",
            "ALTER TABLE employment_record ADD COLUMN responsibilities_embedding ARRAY<FLOAT32>",
        ])
        print("    ℹ️  Columns added as ARRAY<FLOAT32>. Populate via Vertex AI Embedding API later.")
    except Exception as e:
        print(f"    ⚠️  Skipped — {e}")
        print("    ℹ️  Embedding columns can be added later when needed.")


# ═══════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════

def main():
    client = spanner.Client(project=PROJECT)
    instance = client.instance(INSTANCE)
    db = instance.database(DATABASE)

    print(f"Target: {PROJECT}/{INSTANCE}/{DATABASE}")

    # ── P0: Data Cleanup ────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("P0 — Data Cleanup + Coverage Metadata")
    print("=" * 60)

    step_2_1_normalize_supply_chain_position(db)
    step_2_2_deduplicate_role_function(db)
    step_2_3_add_expert_count(db)
    step_2_4_add_artifact_count(db)

    # ── P1: Entity Disambiguation + Routing Signals ─────────────────
    print("\n" + "=" * 60)
    print("P1 — Entity Disambiguation + Routing Signals")
    print("=" * 60)

    step_2_5_add_ambiguity_flag(db)
    step_2_6_create_company_alias(db)
    step_2_6b_create_edge_is_alias_of(db)
    step_2_8_add_expected_resolution(db)
    step_2_9_add_function_confidence(db)
    step_2_7_update_property_graph(db)  # must be last in P1

    # ── P2: Keyword Edges + Confidence + Embeddings ─────────────────
    print("\n" + "=" * 60)
    print("P2 — Missing Keyword Edges + Confidence + Embeddings")
    print("=" * 60)

    step_2_10_create_edge_maps_to_function(db)
    step_2_11_create_edge_maps_to_product_category(db)
    step_2_12_add_selection_count(db)
    step_2_13_add_embedding_columns(db)

    print("\n" + "=" * 60)
    print("✅ Part 2 complete — all schema changes applied!")
    print("=" * 60)


if __name__ == "__main__":
    main()
