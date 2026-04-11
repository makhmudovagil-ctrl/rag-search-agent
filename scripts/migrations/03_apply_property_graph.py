"""
Step 3: Apply Property Graph definition to the destination database.

This must run AFTER data is loaded (Step 2) because Spanner validates
referential integrity of edge SOURCE/DESTINATION references on creation.
"""

from google.cloud import spanner

# ── Config ──────────────────────────────────────────────────────────
DST_PROJECT = "gcp-poc-488614"
DST_INSTANCE = "kg-dev-instance"
DST_DATABASE = "kg_products_v4_dev"

PROPERTY_GRAPH_DDL = """CREATE OR REPLACE PROPERTY GRAPH kg_graph
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
        linkedin_url, market_position, name_normalised, size, website),
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
        vendor_company_id, vendor_name),
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
      LABEL HAS_ROLE PROPERTIES(employment_id, role_id),
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
      LABEL MAPS_TO_INDUSTRY PROPERTIES(industry_id, keyword_id),
    edge_maps_to_product
      KEY(keyword_id, product_id)
      SOURCE KEY(keyword_id) REFERENCES keyword(keyword_id)
      DESTINATION KEY(product_id) REFERENCES product(product_id)
      LABEL MAPS_TO_PRODUCT PROPERTIES(keyword_id, product_id),
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
        validated_in_artifact_id)
  )"""


def main():
    client = spanner.Client(project=DST_PROJECT)
    instance = client.instance(DST_INSTANCE)
    database = instance.database(DST_DATABASE)

    print(f"Applying Property Graph to {DST_PROJECT}/{DST_INSTANCE}/{DST_DATABASE}...")

    operation = database.update_ddl([PROPERTY_GRAPH_DDL])
    print("  Waiting for DDL update...")
    operation.result(timeout=300)

    print("  ✅ Property Graph 'kg_graph' applied successfully")


if __name__ == "__main__":
    main()
