"""
Step 1: Create Spanner instance and database in the target project.

Creates:
  - Instance: kg-dev-instance (100 processing units, us-central1)
  - Database: kg_products_v4_dev (all tables, NO Property Graph yet)

Property Graph is applied AFTER data load (Step 3) because graph
definition validates referential integrity on creation.
"""

from google.cloud import spanner
from google.cloud.spanner_admin_instance_v1.types import Instance

# ── Config ──────────────────────────────────────────────────────────
DST_PROJECT = "gcp-poc-488614"
DST_INSTANCE = "kg-dev-instance"
DST_DATABASE = "kg_products_v4_dev"
DST_CONFIG = "regional-us-central1"
PROCESSING_UNITS = 100  # minimum — ~$117/month (Enterprise edition)

# ── DDL (tables only — no Property Graph) ───────────────────────────
# Exported from kg-poc-489015/kg-poc-instance/kg_products_v4
DDL_STATEMENTS = [
    # ── Node tables ─────────────────────────────────────────────────
    """CREATE TABLE angle (
        angle_id STRING(36) NOT NULL,
        name STRING(256),
        type STRING(64),
        tenure STRING(32),
        scope STRING(MAX),
        angle_bio STRING(MAX),
        priority INT64,
        customers_of_providers BOOL,
    ) PRIMARY KEY(angle_id)""",

    """CREATE TABLE company (
        company_id STRING(256) NOT NULL,
        company_name STRING(512),
        name_normalised STRING(512),
        linkedin_url STRING(512),
        website STRING(512),
        size STRING(64),
        founded INT64,
        description STRING(MAX),
        market_position STRING(32),
        company_type STRING(32),
    ) PRIMARY KEY(company_id)""",

    """CREATE TABLE competitive_set (
        competitive_set_id STRING(128) NOT NULL,
        project_id STRING(36),
        name STRING(256),
        definition STRING(MAX),
        perspective STRING(32),
        customer_segment STRING(MAX),
        use_cases STRING(MAX),
        validated_in_artifact_id STRING(128),
    ) PRIMARY KEY(competitive_set_id)""",

    """CREATE TABLE employment_record (
        employment_id STRING(36) NOT NULL,
        expert_id STRING(36) NOT NULL,
        company_id STRING(256),
        position STRING(512),
        jobtitle_raw STRING(512),
        start_date STRING(16),
        end_date STRING(16),
        start_year INT64,
        start_month INT64,
        end_year INT64,
        end_month INT64,
        is_current BOOL,
        responsibilities STRING(MAX),
        geo STRING(512),
    ) PRIMARY KEY(employment_id)""",

    """CREATE TABLE expert (
        expert_id STRING(36) NOT NULL,
        expert_name STRING(512),
        is_active BOOL,
        skills STRING(MAX),
    ) PRIMARY KEY(expert_id)""",

    """CREATE TABLE geography (
        geography_id STRING(256) NOT NULL,
        name STRING(512),
        level STRING(32),
        parent_id STRING(256),
    ) PRIMARY KEY(geography_id)""",

    """CREATE TABLE industry (
        industry_id STRING(256) NOT NULL,
        name STRING(256),
        description STRING(MAX),
    ) PRIMARY KEY(industry_id)""",

    """CREATE TABLE keyword (
        keyword_id STRING(256) NOT NULL,
        keyword STRING(512),
        source STRING(64),
    ) PRIMARY KEY(keyword_id)""",

    """CREATE TABLE knowledge_artifact (
        artifact_id STRING(128) NOT NULL,
        artifact_type STRING(64),
        date STRING(32),
        call_id STRING(128),
        text STRING(MAX),
        lens STRING(64),
        lens_rationale STRING(MAX),
        expert_id STRING(36),
        project_id STRING(36),
        relevant_employment_id STRING(36),
    ) PRIMARY KEY(artifact_id)""",

    """CREATE TABLE product (
        product_id STRING(256) NOT NULL,
        product_name STRING(512),
        vendor_name STRING(512),
        vendor_company_id STRING(256),
        source_artifact_id STRING(128),
        source STRING(64),
    ) PRIMARY KEY(product_id)""",

    """CREATE TABLE product_category (
        product_category_id STRING(256) NOT NULL,
        name STRING(256),
        category_parent STRING(256),
    ) PRIMARY KEY(product_category_id)""",

    """CREATE TABLE project (
        project_id STRING(36) NOT NULL,
        project_name STRING(512),
        created_at STRING(32),
        brief_object STRING(MAX),
        project_type_raw STRING(64),
        software_focus BOOL,
        client_company_id STRING(256),
        client_company_name STRING(512),
        brief_dimension STRING(MAX),
        brief_lens STRING(MAX),
        brief_driver STRING(MAX),
    ) PRIMARY KEY(project_id)""",

    """CREATE TABLE role (
        role_id STRING(256) NOT NULL,
        role_name STRING(256),
        function STRING(64),
        seniority STRING(64),
        org_scope STRING(64),
    ) PRIMARY KEY(role_id)""",

    """CREATE TABLE subindustry (
        subindustry_id STRING(256) NOT NULL,
        name STRING(256),
        definition STRING(MAX),
        industry_ids STRING(MAX),
    ) PRIMARY KEY(subindustry_id)""",

    # ── Edge tables ─────────────────────────────────────────────────
    """CREATE TABLE edge_angle_targets_company (
        angle_id STRING(36) NOT NULL,
        company_id STRING(256) NOT NULL,
    ) PRIMARY KEY(angle_id, company_id)""",

    """CREATE TABLE edge_angle_targets_industry (
        angle_id STRING(36) NOT NULL,
        industry_id STRING(256) NOT NULL,
    ) PRIMARY KEY(angle_id, industry_id)""",

    """CREATE TABLE edge_angle_targets_product (
        angle_id STRING(36) NOT NULL,
        product_id STRING(256) NOT NULL,
        source STRING(32),
    ) PRIMARY KEY(angle_id, product_id)""",

    """CREATE TABLE edge_angle_targets_subindustry (
        angle_id STRING(36) NOT NULL,
        subindustry_id STRING(256) NOT NULL,
    ) PRIMARY KEY(angle_id, subindustry_id)""",

    """CREATE TABLE edge_at_company (
        employment_id STRING(36) NOT NULL,
        company_id STRING(256) NOT NULL,
    ) PRIMARY KEY(employment_id)""",

    """CREATE TABLE edge_based_in_country (
        employment_id STRING(36) NOT NULL,
        geography_id STRING(256) NOT NULL,
    ) PRIMARY KEY(employment_id, geography_id)""",

    """CREATE TABLE edge_belongs_to_category (
        product_id STRING(256) NOT NULL,
        product_category_id STRING(256) NOT NULL,
    ) PRIMARY KEY(product_id, product_category_id)""",

    """CREATE TABLE edge_competitive_set_for (
        competitive_set_id STRING(128) NOT NULL,
        project_id STRING(36) NOT NULL,
    ) PRIMARY KEY(competitive_set_id, project_id)""",

    """CREATE TABLE edge_competitor_of (
        company_id_a STRING(256) NOT NULL,
        company_id_b STRING(256) NOT NULL,
        source STRING(32),
    ) PRIMARY KEY(company_id_a, company_id_b)""",

    """CREATE TABLE edge_covers (
        employment_id STRING(36) NOT NULL,
        geography_id STRING(256) NOT NULL,
    ) PRIMARY KEY(employment_id, geography_id)""",

    """CREATE TABLE edge_customer_of (
        from_company_id STRING(256) NOT NULL,
        to_company_id STRING(256) NOT NULL,
        status STRING(32),
        end_date STRING(16),
        validated_in_artifact_id STRING(128),
    ) PRIMARY KEY(from_company_id, to_company_id)""",

    """CREATE TABLE edge_features (
        artifact_id STRING(128) NOT NULL,
        expert_id STRING(36) NOT NULL,
    ) PRIMARY KEY(artifact_id, expert_id)""",

    """CREATE TABLE edge_for_angle (
        artifact_id STRING(128) NOT NULL,
        angle_id STRING(36) NOT NULL,
    ) PRIMARY KEY(artifact_id, angle_id)""",

    """CREATE TABLE edge_for_project (
        artifact_id STRING(128) NOT NULL,
        project_id STRING(36) NOT NULL,
    ) PRIMARY KEY(artifact_id, project_id)""",

    """CREATE TABLE edge_has_employment (
        expert_id STRING(36) NOT NULL,
        employment_id STRING(36) NOT NULL,
    ) PRIMARY KEY(expert_id, employment_id)""",

    """CREATE TABLE edge_has_role (
        employment_id STRING(36) NOT NULL,
        role_id STRING(256) NOT NULL,
    ) PRIMARY KEY(employment_id, role_id)""",

    """CREATE TABLE edge_in_competitive_set (
        product_id STRING(256) NOT NULL,
        competitive_set_id STRING(128) NOT NULL,
    ) PRIMARY KEY(product_id, competitive_set_id)""",

    """CREATE TABLE edge_in_industry (
        company_id STRING(256) NOT NULL,
        industry_id STRING(256) NOT NULL,
    ) PRIMARY KEY(company_id, industry_id)""",

    """CREATE TABLE edge_in_subindustry (
        company_id STRING(256) NOT NULL,
        subindustry_id STRING(256) NOT NULL,
    ) PRIMARY KEY(company_id, subindustry_id)""",

    """CREATE TABLE edge_involved_with (
        employment_id STRING(36) NOT NULL,
        product_id STRING(256) NOT NULL,
        source_artifact_id STRING(128) NOT NULL,
        supply_chain_position STRING(32),
        is_user BOOL,
        is_evaluator BOOL,
        is_key_decision_maker BOOL,
        status STRING(16),
        start_date STRING(16),
        end_date STRING(16),
        attachment_method STRING(32),
        validation_source STRING(32),
        notes STRING(MAX),
    ) PRIMARY KEY(employment_id, product_id, source_artifact_id)""",

    """CREATE TABLE edge_maps_to_industry (
        keyword_id STRING(256) NOT NULL,
        industry_id STRING(256) NOT NULL,
    ) PRIMARY KEY(keyword_id, industry_id)""",

    """CREATE TABLE edge_maps_to_product (
        keyword_id STRING(256) NOT NULL,
        product_id STRING(256) NOT NULL,
    ) PRIMARY KEY(keyword_id, product_id)""",

    """CREATE TABLE edge_mentioned_in (
        keyword_id STRING(256) NOT NULL,
        artifact_id STRING(128) NOT NULL,
    ) PRIMARY KEY(keyword_id, artifact_id)""",

    """CREATE TABLE edge_produced_by (
        product_id STRING(256) NOT NULL,
        company_id STRING(256) NOT NULL,
    ) PRIMARY KEY(product_id, company_id)""",

    """CREATE TABLE edge_project_has_angle (
        project_id STRING(36) NOT NULL,
        angle_id STRING(36) NOT NULL,
    ) PRIMARY KEY(project_id, angle_id)""",

    """CREATE TABLE edge_relevant_employment (
        artifact_id STRING(128) NOT NULL,
        employment_id STRING(36) NOT NULL,
        context_type STRING(32),
    ) PRIMARY KEY(artifact_id, employment_id)""",

    """CREATE TABLE edge_selected_for (
        expert_id STRING(36) NOT NULL,
        project_id STRING(36) NOT NULL,
        qualification_id STRING(36),
        angle_id STRING(36),
        selection_status STRING(32),
        relevant_employment_id STRING(36),
        selected_at STRING(32),
    ) PRIMARY KEY(expert_id, project_id)""",

    """CREATE TABLE edge_subindustry_of (
        subindustry_id STRING(256) NOT NULL,
        industry_id STRING(256) NOT NULL,
    ) PRIMARY KEY(subindustry_id, industry_id)""",

    """CREATE TABLE edge_supplier_of (
        from_company_id STRING(256) NOT NULL,
        to_company_id STRING(256) NOT NULL,
        status STRING(32),
        end_date STRING(16),
        validated_in_artifact_id STRING(128),
    ) PRIMARY KEY(from_company_id, to_company_id)""",
]


def main():
    client = spanner.Client(project=DST_PROJECT)

    # ── Step 1: Create instance ─────────────────────────────────────
    print(f"Creating instance '{DST_INSTANCE}' in {DST_PROJECT}...")
    instance = client.instance(
        DST_INSTANCE,
        configuration_name=f"projects/{DST_PROJECT}/instanceConfigs/{DST_CONFIG}",
        display_name="KG Dev Instance",
        processing_units=PROCESSING_UNITS,
        edition=Instance.Edition.ENTERPRISE,
    )

    operation = instance.create()
    print("  Waiting for instance creation...")
    operation.result(timeout=300)
    print(f"  ✅ Instance '{DST_INSTANCE}' created ({PROCESSING_UNITS} PU, {DST_CONFIG})")

    # ── Step 2: Create database with DDL ────────────────────────────
    print(f"\nCreating database '{DST_DATABASE}'...")
    database = instance.database(DST_DATABASE, ddl_statements=DDL_STATEMENTS)

    operation = database.create()
    print("  Waiting for database creation...")
    operation.result(timeout=300)
    print(f"  ✅ Database '{DST_DATABASE}' created with {len(DDL_STATEMENTS)} tables")


if __name__ == "__main__":
    main()
