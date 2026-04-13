"""
Step 14: Add missing schema elements to kg_products_v5_dev.

Adds only what is genuinely missing from v5 (not present in the loaded JSON data):
  1. Embedding columns for vector search (text_embedding, responsibilities_embedding)
  2. Company ambiguity columns (expert_count, ambiguity_flag)
  3. company_alias + edge_is_alias_of tables (for entity disambiguation)
  4. edge_maps_to_function, edge_maps_to_industry, edge_maps_to_product_category
     (for keyword expansion)
  5. Updated Property Graph with new nodes/edges/columns
  6. DML: populate expert_count from edge_at_company + edge_has_employment

Must run AFTER step 13 (Property Graph) because we do CREATE OR REPLACE.
"""

import os
from google.cloud import spanner

PROJECT = os.getenv("GCP_PROJECT_ID", "gcp-poc-488614")
INSTANCE = os.getenv("SPANNER_INSTANCE_ID", "kg-dev-instance")
DATABASE = os.getenv("SPANNER_DATABASE_ID", "kg_products_v5_dev")


# ── DDL statements ──────────────────────────────────────────────────

DDL_STATEMENTS = [
    # 1. Embedding columns
    "ALTER TABLE knowledge_artifact ADD COLUMN text_embedding ARRAY<FLOAT32>(vector_length=>768)",
    "ALTER TABLE employment_record ADD COLUMN responsibilities_embedding ARRAY<FLOAT32>(vector_length=>768)",

    # 2. Company ambiguity columns
    "ALTER TABLE company ADD COLUMN expert_count INT64",
    "ALTER TABLE company ADD COLUMN ambiguity_flag BOOL DEFAULT (false)",

    # 3. company_alias table + indexes
    """CREATE TABLE company_alias (
        alias_id   STRING(256) NOT NULL,
        company_id STRING(36),
        alias_name STRING(512) NOT NULL,
        alias_type STRING(32),
    ) PRIMARY KEY(alias_id)""",
    "CREATE INDEX idx_alias_name ON company_alias(alias_name)",
    "CREATE INDEX idx_alias_company ON company_alias(company_id)",

    # 4. edge_is_alias_of (for Property Graph)
    """CREATE TABLE edge_is_alias_of (
        alias_id   STRING(256) NOT NULL,
        company_id STRING(36) NOT NULL,
    ) PRIMARY KEY(alias_id, company_id)""",

    # 5. edge_maps_to_function (D3 keyword expansion)
    """CREATE TABLE edge_maps_to_function (
        keyword_id    STRING(256) NOT NULL,
        role_function STRING(64)  NOT NULL,
        confidence    FLOAT64,
    ) PRIMARY KEY(keyword_id, role_function)""",

    # 6. edge_maps_to_industry (keyword expansion)
    """CREATE TABLE edge_maps_to_industry (
        keyword_id  STRING(256) NOT NULL,
        industry_id STRING(256) NOT NULL,
    ) PRIMARY KEY(keyword_id, industry_id)""",

    # 7. edge_maps_to_product_category (D4 keyword expansion)
    """CREATE TABLE edge_maps_to_product_category (
        keyword_id          STRING(256) NOT NULL,
        product_category_id STRING(256) NOT NULL,
        confidence          FLOAT64,
    ) PRIMARY KEY(keyword_id, product_category_id)""",
]


# ── Updated Property Graph (adds company_alias, edge_is_alias_of, new columns) ─

PROPERTY_GRAPH_DDL = """CREATE OR REPLACE PROPERTY GRAPH kg_graph
  NODE TABLES(
    angle
      KEY(angle_id)
      LABEL Angle PROPERTIES(
        angle_id, name, type, tenure, scope, angle_bio,
        priority, customers_of_providers),
    company
      KEY(company_id)
      LABEL Company PROPERTIES(
        company_id, name_raw, name_normalised, linkedin_url,
        website, size, founded, description, market_position, company_type,
        expert_count, ambiguity_flag),
    company_alias
      KEY(alias_id)
      LABEL CompanyAlias PROPERTIES(
        alias_id, company_id, alias_name, alias_type),
    competitive_set
      KEY(cs_id)
      LABEL CompetitiveSet PROPERTIES(
        cs_id, perspective, customer_segment, use_cases,
        validated_in_artifact_id, project_id),
    employment_record
      KEY(employment_id)
      LABEL EmploymentRecord PROPERTIES(
        employment_id, expert_id, company_id, company, position,
        jobtitle_raw, start_date, end_date, start_year, start_month,
        end_year, end_month, is_current, responsibilities, geo),
    expert
      KEY(expert_id)
      LABEL Expert PROPERTIES(
        expert_id, name, is_active, skills),
    geography
      KEY(geography_id)
      LABEL Geography PROPERTIES(
        geography_id, name, level, parent_id),
    industry
      KEY(industry_id)
      LABEL Industry PROPERTIES(
        industry_id, name, description),
    keyword
      KEY(keyword_id)
      LABEL Keyword PROPERTIES(
        keyword_id, term_string, source),
    knowledge_artifact
      KEY(artifact_id)
      LABEL KnowledgeArtifact PROPERTIES(
        artifact_id, artifact_type, date, call_id, text,
        lens, lens_rationale, expert_id, project_id),
    product
      KEY(product_id)
      LABEL Product PROPERTIES(
        product_id, product_name, vendor_name,
        vendor_company_id, source_artifact_id),
    product_category
      KEY(product_category_id)
      LABEL ProductCategory PROPERTIES(
        product_category_id, name, category_parent),
    project
      KEY(project_id)
      LABEL Project PROPERTIES(
        project_id, name, created_at, brief_object, project_type_raw,
        software_focus, client_company_id, client_company_name, brief_dimension),
    role
      KEY(role_id)
      LABEL Role PROPERTIES(
        role_id, role_name, `function` AS `function`, seniority, org_scope),
    subindustry
      KEY(subindustry_id)
      LABEL SubIndustry PROPERTIES(
        subindustry_id, name, definition, industry_ids)
  )
  EDGE TABLES(
    edge_has_employment
      KEY(expert_id, employment_id)
      SOURCE KEY(expert_id) REFERENCES expert(expert_id)
      DESTINATION KEY(employment_id) REFERENCES employment_record(employment_id)
      LABEL HAS_EMPLOYMENT PROPERTIES(expert_id, employment_id),
    edge_at_company
      KEY(employment_id, company_id)
      SOURCE KEY(employment_id) REFERENCES employment_record(employment_id)
      DESTINATION KEY(company_id) REFERENCES company(company_id)
      LABEL AT_COMPANY PROPERTIES(employment_id, company_id),
    edge_in_role
      KEY(employment_id, role_id)
      SOURCE KEY(employment_id) REFERENCES employment_record(employment_id)
      DESTINATION KEY(role_id) REFERENCES role(role_id)
      LABEL IN_ROLE PROPERTIES(employment_id, role_id),
    edge_based_in
      KEY(employment_id, geography_id)
      SOURCE KEY(employment_id) REFERENCES employment_record(employment_id)
      DESTINATION KEY(geography_id) REFERENCES geography(geography_id)
      LABEL BASED_IN PROPERTIES(employment_id, geography_id),
    edge_covers
      KEY(employment_id, geography_id)
      SOURCE KEY(employment_id) REFERENCES employment_record(employment_id)
      DESTINATION KEY(geography_id) REFERENCES geography(geography_id)
      LABEL COVERS PROPERTIES(employment_id, geography_id),
    edge_in_industry
      KEY(company_id, industry_id)
      SOURCE KEY(company_id) REFERENCES company(company_id)
      DESTINATION KEY(industry_id) REFERENCES industry(industry_id)
      LABEL IN_INDUSTRY PROPERTIES(company_id, industry_id),
    edge_in_subindustry
      KEY(company_id, subindustry_id)
      SOURCE KEY(company_id) REFERENCES company(company_id)
      DESTINATION KEY(subindustry_id) REFERENCES subindustry(subindustry_id)
      LABEL IN_SUBINDUSTRY PROPERTIES(company_id, subindustry_id),
    edge_involved_with
      KEY(employment_id, product_id, source_artifact_id)
      SOURCE KEY(employment_id) REFERENCES employment_record(employment_id)
      DESTINATION KEY(product_id) REFERENCES product(product_id)
      LABEL INVOLVED_WITH PROPERTIES(
        employment_id, product_id, source_artifact_id,
        is_user, is_evaluator, is_key_decision_maker,
        supply_chain_position, start_date, end_date,
        attachment_method, validation_source, notes),
    edge_competitor_of
      KEY(company_id_a, company_id_b)
      SOURCE KEY(company_id_a) REFERENCES company(company_id)
      DESTINATION KEY(company_id_b) REFERENCES company(company_id)
      LABEL COMPETITOR_OF PROPERTIES(company_id_a, company_id_b, source, source_artifact_id),
    edge_customer_of
      KEY(buyer_company_id, seller_company_id)
      SOURCE KEY(buyer_company_id) REFERENCES company(company_id)
      DESTINATION KEY(seller_company_id) REFERENCES company(company_id)
      LABEL CUSTOMER_OF PROPERTIES(
        buyer_company_id, seller_company_id, status, end_date,
        validated_in_artifact_id, validated),
    edge_supplier_of
      KEY(supplier_company_id, buyer_company_id)
      SOURCE KEY(supplier_company_id) REFERENCES company(company_id)
      DESTINATION KEY(buyer_company_id) REFERENCES company(company_id)
      LABEL SUPPLIER_OF PROPERTIES(
        supplier_company_id, buyer_company_id, status, end_date,
        validated_in_artifact_id, validated),
    edge_produced_by
      KEY(product_id, company_id)
      SOURCE KEY(product_id) REFERENCES product(product_id)
      DESTINATION KEY(company_id) REFERENCES company(company_id)
      LABEL PRODUCED_BY PROPERTIES(product_id, company_id),
    edge_maps_to_product
      KEY(keyword_id, product_id)
      SOURCE KEY(keyword_id) REFERENCES keyword(keyword_id)
      DESTINATION KEY(product_id) REFERENCES product(product_id)
      LABEL MAPS_TO_PRODUCT PROPERTIES(keyword_id, product_id, match_method),
    edge_mentioned_in
      KEY(keyword_id, artifact_id)
      SOURCE KEY(keyword_id) REFERENCES keyword(keyword_id)
      DESTINATION KEY(artifact_id) REFERENCES knowledge_artifact(artifact_id)
      LABEL MENTIONED_IN PROPERTIES(keyword_id, artifact_id),
    edge_mentions_product
      KEY(artifact_id, product_id)
      SOURCE KEY(artifact_id) REFERENCES knowledge_artifact(artifact_id)
      DESTINATION KEY(product_id) REFERENCES product(product_id)
      LABEL MENTIONS_PRODUCT PROPERTIES(artifact_id, product_id),
    edge_belongs_to_category
      KEY(product_id, category_id)
      SOURCE KEY(product_id) REFERENCES product(product_id)
      DESTINATION KEY(category_id) REFERENCES product_category(product_category_id)
      LABEL BELONGS_TO_CATEGORY PROPERTIES(product_id, category_id),
    edge_in_competitive_set
      KEY(product_id, cs_id)
      SOURCE KEY(product_id) REFERENCES product(product_id)
      DESTINATION KEY(cs_id) REFERENCES competitive_set(cs_id)
      LABEL IN_COMPETITIVE_SET PROPERTIES(product_id, cs_id),
    edge_for_project
      KEY(artifact_id, project_id)
      SOURCE KEY(artifact_id) REFERENCES knowledge_artifact(artifact_id)
      DESTINATION KEY(project_id) REFERENCES project(project_id)
      LABEL FOR_PROJECT PROPERTIES(artifact_id, project_id),
    edge_for_angle
      KEY(artifact_id, angle_id)
      SOURCE KEY(artifact_id) REFERENCES knowledge_artifact(artifact_id)
      DESTINATION KEY(angle_id) REFERENCES angle(angle_id)
      LABEL FOR_ANGLE PROPERTIES(artifact_id, angle_id),
    edge_features
      KEY(artifact_id, expert_id)
      SOURCE KEY(artifact_id) REFERENCES knowledge_artifact(artifact_id)
      DESTINATION KEY(expert_id) REFERENCES expert(expert_id)
      LABEL FEATURES PROPERTIES(artifact_id, expert_id),
    edge_relevant_employment_mentioned
      KEY(artifact_id, employment_id)
      SOURCE KEY(artifact_id) REFERENCES knowledge_artifact(artifact_id)
      DESTINATION KEY(employment_id) REFERENCES employment_record(employment_id)
      LABEL RELEVANT_EMPLOYMENT_MENTIONED PROPERTIES(
        artifact_id, employment_id, context_type, matched_employer_text),
    edge_has_angle
      KEY(project_id, angle_id)
      SOURCE KEY(project_id) REFERENCES project(project_id)
      DESTINATION KEY(angle_id) REFERENCES angle(angle_id)
      LABEL HAS_ANGLE PROPERTIES(project_id, angle_id),
    edge_subindustry_to_industry
      KEY(subindustry_id, industry_id)
      SOURCE KEY(subindustry_id) REFERENCES subindustry(subindustry_id)
      DESTINATION KEY(industry_id) REFERENCES industry(industry_id)
      LABEL SUBINDUSTRY_OF PROPERTIES(subindustry_id, industry_id),
    edge_is_alias_of
      KEY(alias_id, company_id)
      SOURCE KEY(alias_id) REFERENCES company_alias(alias_id)
      DESTINATION KEY(company_id) REFERENCES company(company_id)
      LABEL IS_ALIAS_OF PROPERTIES(alias_id, company_id)
  )"""


# ── DML: populate expert_count ──────────────────────────────────────

EXPERT_COUNT_DML = """
UPDATE company c
SET c.expert_count = (
    SELECT COUNT(DISTINCT ehe.expert_id)
    FROM edge_at_company eac
    JOIN edge_has_employment ehe ON eac.employment_id = ehe.employment_id
    WHERE eac.company_id = c.company_id
)
WHERE TRUE
"""


def main():
    client = spanner.Client(project=PROJECT)
    instance = client.instance(INSTANCE)
    database = instance.database(DATABASE)

    # Step 1: Apply DDL (ALTER TABLE + CREATE TABLE)
    print(f"[14] Applying schema additions to {PROJECT}/{INSTANCE}/{DATABASE}...")
    print(f"  {len(DDL_STATEMENTS)} DDL statements")

    operation = database.update_ddl(DDL_STATEMENTS)
    print("  Waiting for DDL update...")
    operation.result(timeout=600)
    print("  DDL applied successfully")

    # Step 2: Apply updated Property Graph
    print("  Applying updated Property Graph...")
    operation = database.update_ddl([PROPERTY_GRAPH_DDL])
    operation.result(timeout=300)
    print("  Property Graph updated successfully")

    # Step 3: Populate expert_count via DML
    print("  Populating expert_count...")
    row_count = 0

    def run_dml(transaction):
        nonlocal row_count
        row_count = transaction.execute_update(EXPERT_COUNT_DML)

    database.run_in_transaction(run_dml)
    print(f"  expert_count populated for {row_count} companies")

    print("  All done.")


if __name__ == "__main__":
    main()
