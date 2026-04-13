"""
Part 2b: Fix gaps between current schema and document recommendations.

Fixes:
  1. Role taxonomy: 15 → 11 values (Section 5.2 #5)
     Merge: Trading→Commercial, Advisory→Commercial, Board→C-Suite,
            Marketing→Commercial, Legal-Regulatory→Legal
  2. Company type: standardize NULL/empty → 'Unknown' (Section 8)
  3. Property Graph: add edge_maps_to_function + edge_maps_to_product_category (Section 5.1 #4)
  4. Materialized Views for LLM (Section 5.1 #1)

Each step is idempotent — safe to re-run.
"""

import subprocess
from google.cloud import spanner
from google.oauth2.credentials import Credentials

# ── Config ──────────────────────────────────────────────────────────
PROJECT = "gcp-poc-488614"
INSTANCE = "kg-dev-instance"
DATABASE = "kg_products_v4_dev"


def get_client():
    token = subprocess.check_output([
        r"C:\Users\ehasanov\AppData\Local\Google\Cloud SDK"
        r"\google-cloud-sdk\bin\gcloud.cmd",
        "auth", "print-access-token",
    ], text=True).strip()
    creds = Credentials(token=token)
    return spanner.Client(project=PROJECT, credentials=creds)


def run_dml(db, label: str, statements: list[str]):
    def _work(transaction):
        for stmt in statements:
            row_count = transaction.execute_update(stmt)
            print(f"    {row_count} rows affected")
    print(f"  {label}...")
    db.run_in_transaction(_work)


def run_ddl(db, label: str, statements: list[str]):
    print(f"  {label}...")
    operation = db.update_ddl(statements)
    operation.result(timeout=300)
    print(f"    DDL applied ({len(statements)} statement(s))")


# ═══════════════════════════════════════════════════════════════════
# Fix 1: Role taxonomy — 15 → 11 values
#
# Document Section 5.2 #5: "Commit immediately to the fixed 11-value
# taxonomy proposed in schema v15."
#
# Current 15 values → Target 11:
#   Keep as-is (9): Operations, Commercial, Finance, Strategy,
#                    Engineering, IT, R&D, Procurement, HR
#   Merge:
#     Advisory (572 rows)        → Strategy  (advisory roles are strategic)
#     Trading (59 rows)          → Commercial (trading is commercial activity)
#     Board (2 rows)             → C-Suite   (board-level = C-Suite)
#     Marketing (7 rows)         → Commercial (marketing is commercial)
#     Legal-Regulatory (177 rows)→ Legal      (rename, cleaner label)
#   Rename:
#     C-Suite (2 rows)           stays C-Suite
#     Legal-Regulatory → Legal
#
#   Final 11: Operations, Commercial, Finance, Strategy, Engineering,
#             IT, R&D, Procurement, HR, C-Suite, Legal
# ═══════════════════════════════════════════════════════════════════

def fix_1_role_taxonomy(db):
    """Consolidate role.function from 15 to 11 values."""
    print("\n[Fix 1] Role taxonomy: 15 -> 11 values")

    merges = [
        ("Advisory -> Strategy",
         "UPDATE role SET `function` = 'Strategy' WHERE `function` = 'Advisory'"),
        ("Trading -> Commercial",
         "UPDATE role SET `function` = 'Commercial' WHERE `function` = 'Trading'"),
        ("Board -> C-Suite",
         "UPDATE role SET `function` = 'C-Suite' WHERE `function` = 'Board'"),
        ("Marketing -> Commercial",
         "UPDATE role SET `function` = 'Commercial' WHERE `function` = 'Marketing'"),
        ("Legal-Regulatory -> Legal",
         "UPDATE role SET `function` = 'Legal' WHERE `function` = 'Legal-Regulatory'"),
    ]

    for label, stmt in merges:
        run_dml(db, label, [stmt])

    # Verify
    with db.snapshot() as snap:
        results = snap.execute_sql(
            "SELECT `function`, COUNT(*) as cnt FROM role "
            "GROUP BY `function` ORDER BY cnt DESC"
        )
        vals = []
        print("    Final values:")
        for row in results:
            print(f"      {str(row[0]):20s} -> {row[1]}")
            if row[0] is not None:
                vals.append(row[0])
        print(f"    Total distinct (non-null): {len(vals)}")


# ═══════════════════════════════════════════════════════════════════
# Fix 2: company_type — standardize NULL/empty
# ═══════════════════════════════════════════════════════════════════

def fix_2_company_type(db):
    """Standardize NULL/empty company_type → 'Unknown'."""
    print("\n[Fix 2] company_type: NULL/empty -> 'Unknown'")

    run_dml(db, "NULL -> Unknown", [
        "UPDATE company SET company_type = 'Unknown' "
        "WHERE company_type IS NULL OR company_type = ''"
    ])

    # Verify
    with db.snapshot() as snap:
        results = snap.execute_sql(
            "SELECT company_type, COUNT(*) as cnt FROM company "
            "GROUP BY company_type ORDER BY cnt DESC"
        )
        print("    Final values:")
        for row in results:
            print(f"      {str(row[0]):20s} -> {row[1]}")


# ═══════════════════════════════════════════════════════════════════
# Fix 3: Add edge_maps_to_function + edge_maps_to_product_category
#         to Property Graph
# ═══════════════════════════════════════════════════════════════════

def fix_3_update_property_graph(db):
    """Update Property Graph to include all edge tables."""
    print("\n[Fix 3] Update Property Graph (add missing edges)")

    # Full graph redefinition — same as step_2_7 but with 2 new edges
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
        keyword, keyword_id, source, selection_count),
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
        selection_status, relevant_employment_id, selected_at),
    edge_maps_to_function
      KEY(keyword_id, role_function)
      SOURCE KEY(keyword_id) REFERENCES keyword(keyword_id)
      DESTINATION KEY(role_function) REFERENCES role(role_id)
      LABEL MAPS_TO_FUNCTION PROPERTIES(keyword_id, role_function, confidence),
    edge_maps_to_product_category
      KEY(keyword_id, product_category_id)
      SOURCE KEY(keyword_id) REFERENCES keyword(keyword_id)
      DESTINATION KEY(product_category_id) REFERENCES product_category(product_category_id)
      LABEL MAPS_TO_PRODUCT_CATEGORY PROPERTIES(keyword_id, product_category_id, confidence)
  )"""

    run_ddl(db, "CREATE OR REPLACE PROPERTY GRAPH", [ddl])


# ═══════════════════════════════════════════════════════════════════
# Fix 4: Materialized LLM Views (Section 5.1 #1)
#
# Simplified views so LLM needs fewer joins and less syntax.
# ═══════════════════════════════════════════════════════════════════

def fix_4_create_views(db):
    """Create materialized views for LLM agent simplification."""
    print("\n[Fix 4] Create Materialized LLM Views")

    views = [
        # View 1: Decision makers — flatten INVOLVED_WITH where is_key_decision_maker=true
        ("DecisionMakers_View", """CREATE OR REPLACE VIEW DecisionMakers_View SQL SECURITY INVOKER AS
SELECT
    e.expert_id,
    e.expert_name,
    er.employment_id,
    er.position,
    er.responsibilities,
    er.is_current,
    c.company_id,
    c.company_name,
    c.company_type,
    p.product_id,
    p.product_name,
    eiw.supply_chain_position,
    eiw.start_date AS involvement_start,
    eiw.end_date AS involvement_end,
    eiw.status AS involvement_status
FROM expert e
JOIN edge_has_employment ehe ON e.expert_id = ehe.expert_id
JOIN employment_record er ON ehe.employment_id = er.employment_id
JOIN edge_at_company eac ON er.employment_id = eac.employment_id
JOIN company c ON eac.company_id = c.company_id
JOIN edge_involved_with eiw ON er.employment_id = eiw.employment_id
JOIN product p ON eiw.product_id = p.product_id
WHERE eiw.is_key_decision_maker = true"""),

        # View 2: Buyers — flatten INVOLVED_WITH where supply_chain_position='buyer'
        ("Buyers_View", """CREATE OR REPLACE VIEW Buyers_View SQL SECURITY INVOKER AS
SELECT
    e.expert_id,
    e.expert_name,
    er.employment_id,
    er.position,
    er.is_current,
    c.company_id,
    c.company_name,
    p.product_id,
    p.product_name,
    eiw.start_date AS buying_start,
    eiw.end_date AS buying_end,
    eiw.is_evaluator,
    eiw.is_user,
    eiw.status
FROM expert e
JOIN edge_has_employment ehe ON e.expert_id = ehe.expert_id
JOIN employment_record er ON ehe.employment_id = er.employment_id
JOIN edge_at_company eac ON er.employment_id = eac.employment_id
JOIN company c ON eac.company_id = c.company_id
JOIN edge_involved_with eiw ON er.employment_id = eiw.employment_id
JOIN product p ON eiw.product_id = p.product_id
WHERE eiw.supply_chain_position = 'buyer'"""),

        # View 3: Current experts — active experts with current employment
        ("CurrentExperts_View", """CREATE OR REPLACE VIEW CurrentExperts_View SQL SECURITY INVOKER AS
SELECT
    e.expert_id,
    e.expert_name,
    e.skills,
    er.employment_id,
    er.position,
    er.responsibilities,
    er.geo,
    c.company_id,
    c.company_name,
    c.company_type,
    r.role_name,
    r.`function` AS role_function,
    r.seniority
FROM expert e
JOIN edge_has_employment ehe ON e.expert_id = ehe.expert_id
JOIN employment_record er ON ehe.employment_id = er.employment_id
JOIN edge_at_company eac ON er.employment_id = eac.employment_id
JOIN company c ON eac.company_id = c.company_id
LEFT JOIN edge_has_role ehr ON er.employment_id = ehr.employment_id
LEFT JOIN role r ON ehr.role_id = r.role_id
WHERE er.is_current = true AND e.is_active = true"""),

        # View 4: Expert product involvement — flat view of who is involved with what
        ("ExpertProducts_View", """CREATE OR REPLACE VIEW ExpertProducts_View SQL SECURITY INVOKER AS
SELECT
    e.expert_id,
    e.expert_name,
    c.company_name,
    p.product_id,
    p.product_name,
    eiw.supply_chain_position,
    eiw.is_key_decision_maker,
    eiw.is_evaluator,
    eiw.is_user,
    eiw.start_date,
    eiw.end_date,
    eiw.status,
    er.is_current AS currently_employed
FROM expert e
JOIN edge_has_employment ehe ON e.expert_id = ehe.expert_id
JOIN employment_record er ON ehe.employment_id = er.employment_id
JOIN edge_at_company eac ON er.employment_id = eac.employment_id
JOIN company c ON eac.company_id = c.company_id
JOIN edge_involved_with eiw ON er.employment_id = eiw.employment_id
JOIN product p ON eiw.product_id = p.product_id"""),

        # View 5: Company competitors — flat competitor pairs
        ("CompanyCompetitors_View", """CREATE OR REPLACE VIEW CompanyCompetitors_View SQL SECURITY INVOKER AS
SELECT
    c1.company_id AS company_id,
    c1.company_name AS company_name,
    c2.company_id AS competitor_id,
    c2.company_name AS competitor_name,
    eco.source
FROM company c1
JOIN edge_competitor_of eco ON c1.company_id = eco.company_id_a
JOIN company c2 ON eco.company_id_b = c2.company_id"""),
    ]

    for name, ddl in views:
        try:
            run_ddl(db, f"CREATE VIEW {name}", [ddl])
        except Exception as e:
            err = str(e)
            if "already exists" in err.lower() or "Duplicate" in err:
                print(f"    (already exists, skipping)")
            else:
                print(f"    ERROR: {err[:200]}")


# ═══════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════

def main():
    client = get_client()
    instance = client.instance(INSTANCE)
    db = instance.database(DATABASE)

    print(f"Target: {PROJECT}/{INSTANCE}/{DATABASE}")

    fix_1_role_taxonomy(db)
    fix_2_company_type(db)
    fix_3_update_property_graph(db)
    fix_4_create_views(db)

    print("\n" + "=" * 60)
    print("All fixes applied!")
    print("=" * 60)


if __name__ == "__main__":
    main()
