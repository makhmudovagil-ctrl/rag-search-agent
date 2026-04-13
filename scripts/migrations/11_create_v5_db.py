"""
Step 11: Create kg_products_v5_dev database.

Creates database on existing kg-dev-instance with DDL derived
directly from data-v5/ JSON schemas. No schema borrowed from v4.

String sizing strategy:
- UUID PK fields: STRING(36) — UUIDs are always 36 chars
- Non-UUID PK fields: STRING(1024) — generous limit, covers all observed values
  (Spanner does NOT allow STRING(MAX) in primary key columns)
- Non-PK string fields: STRING(MAX) — unconstrained

Property Graph is applied separately in step 13 (after data load).
"""

from google.cloud import spanner

# ── Config ───────────────────────────────────────────────────────────
PROJECT = "gcp-poc-488614"
INSTANCE = "kg-dev-instance"
DATABASE = "kg_products_v5_dev"

DDL_STATEMENTS = [

    # ── Node tables ──────────────────────────────────────────────────

    """CREATE TABLE expert (
        expert_id  STRING(36)  NOT NULL,
        name       STRING(MAX),
        is_active  BOOL,
        skills     STRING(MAX),
    ) PRIMARY KEY(expert_id)""",

    """CREATE TABLE company (
        company_id       STRING(36)  NOT NULL,
        name_raw         STRING(MAX),
        name_normalised  STRING(MAX),
        linkedin_url     STRING(MAX),
        website          STRING(MAX),
        size             STRING(MAX),
        founded          INT64,
        description      STRING(MAX),
        market_position  STRING(MAX),
        company_type     STRING(MAX),
    ) PRIMARY KEY(company_id)""",

    """CREATE TABLE employment_record (
        employment_id    STRING(36)  NOT NULL,
        expert_id        STRING(36),
        company_id       STRING(36),
        company          STRING(MAX),
        position         STRING(MAX),
        jobtitle_raw     STRING(MAX),
        start_date       STRING(MAX),
        end_date         STRING(MAX),
        start_year       INT64,
        start_month      INT64,
        end_year         INT64,
        end_month        INT64,
        is_current       BOOL,
        responsibilities STRING(MAX),
        geo              STRING(MAX),
    ) PRIMARY KEY(employment_id)""",

    # role_id max observed: 89 chars → STRING(1024) PK
    """CREATE TABLE role (
        role_id    STRING(1024) NOT NULL,
        role_name  STRING(MAX),
        `function` STRING(MAX),
        seniority  STRING(MAX),
        org_scope  STRING(MAX),
    ) PRIMARY KEY(role_id)""",

    # industry_id max observed: 32 chars → STRING(1024) PK
    """CREATE TABLE industry (
        industry_id  STRING(1024) NOT NULL,
        name         STRING(MAX),
        description  STRING(MAX),
    ) PRIMARY KEY(industry_id)""",

    # subindustry_id max observed: 27 chars → STRING(1024) PK
    """CREATE TABLE subindustry (
        subindustry_id  STRING(1024) NOT NULL,
        name            STRING(MAX),
        definition      STRING(MAX),
        industry_ids    STRING(MAX),
    ) PRIMARY KEY(subindustry_id)""",

    # product_id max observed: 153 chars → STRING(1024) PK
    """CREATE TABLE product (
        product_id         STRING(1024) NOT NULL,
        product_name       STRING(MAX),
        vendor_name        STRING(MAX),
        vendor_company_id  STRING(36),
        source_artifact_id STRING(MAX),
    ) PRIMARY KEY(product_id)""",

    # product_category_id max observed: 55 chars → STRING(1024) PK
    """CREATE TABLE product_category (
        product_category_id  STRING(1024) NOT NULL,
        name                 STRING(MAX),
        category_parent      STRING(MAX),
    ) PRIMARY KEY(product_category_id)""",

    # geography_id max observed: 20 chars → STRING(1024) PK
    """CREATE TABLE geography (
        geography_id  STRING(1024) NOT NULL,
        name          STRING(MAX),
        level         STRING(MAX),
        parent_id     STRING(MAX),
    ) PRIMARY KEY(geography_id)""",

    # keyword_id max observed: 89 chars → STRING(1024) PK
    """CREATE TABLE keyword (
        keyword_id   STRING(1024) NOT NULL,
        term_string  STRING(MAX),
        source       STRING(MAX),
    ) PRIMARY KEY(keyword_id)""",

    # artifact_id max observed: 77 chars → STRING(1024) PK
    """CREATE TABLE knowledge_artifact (
        artifact_id     STRING(1024) NOT NULL,
        artifact_type   STRING(MAX),
        date            STRING(MAX),
        call_id         STRING(MAX),
        text            STRING(MAX),
        lens            STRING(MAX),
        lens_rationale  STRING(MAX),
        expert_id       STRING(36),
        project_id      STRING(36),
    ) PRIMARY KEY(artifact_id)""",

    # cs_id max observed: 121 chars → STRING(1024) PK
    """CREATE TABLE competitive_set (
        cs_id                    STRING(1024) NOT NULL,
        perspective              STRING(MAX),
        customer_segment         STRING(MAX),
        use_cases                STRING(MAX),
        validated_in_artifact_id STRING(MAX),
        project_id               STRING(36),
    ) PRIMARY KEY(cs_id)""",

    """CREATE TABLE project (
        project_id          STRING(36)  NOT NULL,
        name                STRING(MAX),
        created_at          STRING(MAX),
        brief_object        STRING(MAX),
        project_type_raw    STRING(MAX),
        software_focus      BOOL,
        client_company_id   STRING(36),
        client_company_name STRING(MAX),
        brief_dimension     STRING(MAX),
    ) PRIMARY KEY(project_id)""",

    """CREATE TABLE angle (
        angle_id               STRING(36)  NOT NULL,
        name                   STRING(MAX),
        type                   STRING(MAX),
        tenure                 STRING(MAX),
        scope                  STRING(MAX),
        angle_bio              STRING(MAX),
        priority               INT64,
        customers_of_providers BOOL,
    ) PRIMARY KEY(angle_id)""",

    # ── Edge tables (with data) ───────────────────────────────────────

    """CREATE TABLE edge_has_employment (
        expert_id      STRING(36) NOT NULL,
        employment_id  STRING(36) NOT NULL,
    ) PRIMARY KEY(expert_id, employment_id)""",

    """CREATE TABLE edge_at_company (
        employment_id  STRING(36) NOT NULL,
        company_id     STRING(36) NOT NULL,
    ) PRIMARY KEY(employment_id, company_id)""",

    """CREATE TABLE edge_in_role (
        employment_id  STRING(36)   NOT NULL,
        role_id        STRING(1024) NOT NULL,
    ) PRIMARY KEY(employment_id, role_id)""",

    """CREATE TABLE edge_based_in (
        employment_id  STRING(36)   NOT NULL,
        geography_id   STRING(1024) NOT NULL,
    ) PRIMARY KEY(employment_id, geography_id)""",

    """CREATE TABLE edge_covers (
        employment_id  STRING(36)   NOT NULL,
        geography_id   STRING(1024) NOT NULL,
    ) PRIMARY KEY(employment_id, geography_id)""",

    """CREATE TABLE edge_in_industry (
        company_id   STRING(36)   NOT NULL,
        industry_id  STRING(1024) NOT NULL,
    ) PRIMARY KEY(company_id, industry_id)""",

    """CREATE TABLE edge_in_subindustry (
        company_id     STRING(36)   NOT NULL,
        subindustry_id STRING(1024) NOT NULL,
    ) PRIMARY KEY(company_id, subindustry_id)""",

    """CREATE TABLE edge_involved_with (
        employment_id         STRING(36)   NOT NULL,
        product_id            STRING(1024) NOT NULL,
        source_artifact_id    STRING(1024) NOT NULL,
        is_user               BOOL,
        is_evaluator          BOOL,
        is_key_decision_maker BOOL,
        supply_chain_position STRING(MAX),
        start_date            STRING(MAX),
        end_date              STRING(MAX),
        attachment_method     STRING(MAX),
        validation_source     STRING(MAX),
        notes                 STRING(MAX),
    ) PRIMARY KEY(employment_id, product_id, source_artifact_id)""",

    """CREATE TABLE edge_competitor_of (
        company_id_a       STRING(36) NOT NULL,
        company_id_b       STRING(36) NOT NULL,
        source             STRING(MAX),
        source_artifact_id STRING(MAX),
    ) PRIMARY KEY(company_id_a, company_id_b)""",

    """CREATE TABLE edge_customer_of (
        buyer_company_id         STRING(36) NOT NULL,
        seller_company_id        STRING(36) NOT NULL,
        status                   STRING(MAX),
        end_date                 STRING(MAX),
        validated_in_artifact_id STRING(MAX),
        validated                BOOL,
    ) PRIMARY KEY(buyer_company_id, seller_company_id)""",

    """CREATE TABLE edge_supplier_of (
        supplier_company_id      STRING(36) NOT NULL,
        buyer_company_id         STRING(36) NOT NULL,
        status                   STRING(MAX),
        end_date                 STRING(MAX),
        validated_in_artifact_id STRING(MAX),
        validated                BOOL,
    ) PRIMARY KEY(supplier_company_id, buyer_company_id)""",

    """CREATE TABLE edge_produced_by (
        product_id  STRING(1024) NOT NULL,
        company_id  STRING(36)   NOT NULL,
    ) PRIMARY KEY(product_id, company_id)""",

    """CREATE TABLE edge_maps_to_product (
        keyword_id    STRING(1024) NOT NULL,
        product_id    STRING(1024) NOT NULL,
        match_method  STRING(MAX),
    ) PRIMARY KEY(keyword_id, product_id)""",

    """CREATE TABLE edge_mentioned_in (
        keyword_id   STRING(1024) NOT NULL,
        artifact_id  STRING(1024) NOT NULL,
    ) PRIMARY KEY(keyword_id, artifact_id)""",

    """CREATE TABLE edge_mentions_product (
        artifact_id  STRING(1024) NOT NULL,
        product_id   STRING(1024) NOT NULL,
    ) PRIMARY KEY(artifact_id, product_id)""",

    """CREATE TABLE edge_belongs_to_category (
        product_id   STRING(1024) NOT NULL,
        category_id  STRING(1024) NOT NULL,
    ) PRIMARY KEY(product_id, category_id)""",

    """CREATE TABLE edge_in_competitive_set (
        product_id  STRING(1024) NOT NULL,
        cs_id       STRING(1024) NOT NULL,
    ) PRIMARY KEY(product_id, cs_id)""",

    """CREATE TABLE edge_for_project (
        artifact_id  STRING(1024) NOT NULL,
        project_id   STRING(36)   NOT NULL,
    ) PRIMARY KEY(artifact_id, project_id)""",

    """CREATE TABLE edge_for_angle (
        artifact_id  STRING(1024) NOT NULL,
        angle_id     STRING(36)   NOT NULL,
    ) PRIMARY KEY(artifact_id, angle_id)""",

    """CREATE TABLE edge_features (
        artifact_id  STRING(1024) NOT NULL,
        expert_id    STRING(36)   NOT NULL,
    ) PRIMARY KEY(artifact_id, expert_id)""",

    """CREATE TABLE edge_relevant_employment_mentioned (
        artifact_id           STRING(1024) NOT NULL,
        employment_id         STRING(36)   NOT NULL,
        context_type          STRING(MAX),
        matched_employer_text STRING(MAX),
    ) PRIMARY KEY(artifact_id, employment_id)""",

    """CREATE TABLE edge_has_angle (
        project_id  STRING(36) NOT NULL,
        angle_id    STRING(36) NOT NULL,
    ) PRIMARY KEY(project_id, angle_id)""",

    """CREATE TABLE edge_subindustry_to_industry (
        subindustry_id  STRING(1024) NOT NULL,
        industry_id     STRING(1024) NOT NULL,
    ) PRIMARY KEY(subindustry_id, industry_id)""",

    # Polymorphic edge — excluded from Property Graph, queryable via SQL
    """CREATE TABLE edge_keyword_inference (
        keyword_id          STRING(1024) NOT NULL,
        target_type         STRING(64)   NOT NULL,
        target_id           STRING(1024) NOT NULL,
        context_artifact_id STRING(1024) NOT NULL,
        edge_type           STRING(64)   NOT NULL,
        target_name         STRING(MAX),
        confidence          FLOAT64,
    ) PRIMARY KEY(keyword_id, target_type, target_id, context_artifact_id, edge_type)""",

    # ── Empty edge tables (schema defined, data comes later) ──────────

    """CREATE TABLE edge_angle_maps_to_keyword (
        angle_id    STRING(36)   NOT NULL,
        keyword_id  STRING(1024) NOT NULL,
    ) PRIMARY KEY(angle_id, keyword_id)""",

    """CREATE TABLE edge_angle_targets_company (
        angle_id    STRING(36) NOT NULL,
        company_id  STRING(36) NOT NULL,
    ) PRIMARY KEY(angle_id, company_id)""",

    """CREATE TABLE edge_angle_targets_geo (
        angle_id      STRING(36)   NOT NULL,
        geography_id  STRING(1024) NOT NULL,
    ) PRIMARY KEY(angle_id, geography_id)""",

    """CREATE TABLE edge_angle_targets_industry (
        angle_id     STRING(36)   NOT NULL,
        industry_id  STRING(1024) NOT NULL,
    ) PRIMARY KEY(angle_id, industry_id)""",

    """CREATE TABLE edge_angle_targets_product (
        angle_id    STRING(36)   NOT NULL,
        product_id  STRING(1024) NOT NULL,
    ) PRIMARY KEY(angle_id, product_id)""",

    """CREATE TABLE edge_angle_targets_role (
        angle_id  STRING(36)   NOT NULL,
        role_id   STRING(1024) NOT NULL,
    ) PRIMARY KEY(angle_id, role_id)""",

    """CREATE TABLE edge_angle_targets_subindustry (
        angle_id       STRING(36)   NOT NULL,
        subindustry_id STRING(1024) NOT NULL,
    ) PRIMARY KEY(angle_id, subindustry_id)""",

    """CREATE TABLE edge_relevant_employment (
        artifact_id    STRING(1024) NOT NULL,
        employment_id  STRING(36)   NOT NULL,
        context_type   STRING(MAX),
    ) PRIMARY KEY(artifact_id, employment_id)""",

    """CREATE TABLE edge_selected_for (
        expert_id   STRING(36) NOT NULL,
        project_id  STRING(36) NOT NULL,
    ) PRIMARY KEY(expert_id, project_id)""",
]


def main():
    client = spanner.Client(project=PROJECT)
    instance = client.instance(INSTANCE)

    print(f"Creating database '{DATABASE}' on {PROJECT}/{INSTANCE}...")
    database = instance.database(DATABASE, ddl_statements=DDL_STATEMENTS)

    operation = database.create()
    print("  Waiting for database creation...")
    operation.result(timeout=300)
    print(f"  ✅ Database '{DATABASE}' created with {len(DDL_STATEMENTS)} tables")


if __name__ == "__main__":
    main()
