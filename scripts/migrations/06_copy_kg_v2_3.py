"""
Step 6: Copy kg_v2_3 from source project to destination.

Source:  kg-poc-489015 / kg-poc-instance / kg_v2_3
Dest:    gcp-poc-488614 / kg-dev-instance / kg_v2_3_dev

~2.4M rows across 29 tables. Uses snapshot.read() API + insert_or_update.
Large tables (300K+) use smaller batch size to avoid mutation limits.
"""

import time

from google.cloud import spanner
from google.cloud.spanner_v1 import KeySet

# ── Config ──────────────────────────────────────────────────────────
SRC_PROJECT = "kg-poc-489015"
SRC_INSTANCE = "kg-poc-instance"
SRC_DATABASE = "kg_v2_3"

DST_PROJECT = "gcp-poc-488614"
DST_INSTANCE = "kg-dev-instance"
DST_DATABASE = "kg_v2_3_dev"

BATCH_SIZE_DEFAULT = 500
BATCH_SIZE_LARGE = 200  # for tables with STRING(MAX) columns and 100K+ rows

# ── DDL (tables only, no Property Graph, no FK constraints, no indexes) ──
# Constraints and indexes are added AFTER data load to speed up inserts.
DDL_TABLES = [
    """CREATE TABLE AIInsight (
        insight_id STRING(36) NOT NULL,
        project_id STRING(36),
        insight_type STRING(MAX),
        insight_text STRING(MAX),
    ) PRIMARY KEY(insight_id)""",

    """CREATE TABLE AboutProject (
        transcript_id STRING(36) NOT NULL,
        project_id STRING(36) NOT NULL,
    ) PRIMARY KEY(transcript_id, project_id)""",

    """CREATE TABLE Angle (
        angle_id STRING(36) NOT NULL,
        project_id STRING(36),
        angle_name STRING(MAX),
        angle_type STRING(MAX),
        seniority STRING(MAX),
        tenure STRING(MAX),
        description STRING(MAX),
        location_ids STRING(MAX),
        derived_scope STRING(MAX),
        expert_keywords STRING(MAX),
    ) PRIMARY KEY(angle_id)""",

    """CREATE TABLE AngleCompetitor (
        angle_id STRING(36) NOT NULL,
        company_id STRING(36) NOT NULL,
    ) PRIMARY KEY(angle_id, company_id)""",

    """CREATE TABLE AngleHasCompany (
        angle_id STRING(36) NOT NULL,
        company_id STRING(36) NOT NULL,
    ) PRIMARY KEY(angle_id, company_id)""",

    """CREATE TABLE CallRecord (
        call_id STRING(36) NOT NULL,
        call_datetime STRING(MAX),
        call_duration INT64,
        call_status STRING(MAX),
        recording_file STRING(MAX),
    ) PRIMARY KEY(call_id)""",

    """CREATE TABLE CommissionedBy (
        call_id STRING(36) NOT NULL,
        project_id STRING(36) NOT NULL,
    ) PRIMARY KEY(call_id, project_id)""",

    """CREATE TABLE Company (
        company_id STRING(36) NOT NULL,
        company_display_name STRING(MAX),
        company_normalized_name STRING(MAX),
        company_linkedin STRING(MAX),
    ) PRIMARY KEY(company_id)""",

    """CREATE TABLE DescribesBrief (
        project_id STRING(36) NOT NULL,
        dest_project_id STRING(36) NOT NULL,
    ) PRIMARY KEY(project_id, dest_project_id)""",

    """CREATE TABLE DescribesExpert (
        expert_profile_id STRING(36) NOT NULL,
    ) PRIMARY KEY(expert_profile_id)""",

    """CREATE TABLE DncBlocksCompany (
        dnc_setting_id STRING(36) NOT NULL,
        company_id STRING(36) NOT NULL,
    ) PRIMARY KEY(dnc_setting_id, company_id)""",

    """CREATE TABLE DncSetting (
        dnc_setting_id STRING(36) NOT NULL,
        project_id STRING(36),
        tenure STRING(MAX),
        recency_months INT64,
        setting_index INT64,
    ) PRIMARY KEY(dnc_setting_id)""",

    """CREATE TABLE EmployedAt (
        employment_id STRING(36) NOT NULL,
        expert_profile_id STRING(36) NOT NULL,
        company_id STRING(36) NOT NULL,
        position STRING(MAX),
        role STRING(MAX),
        seniority STRING(MAX),
        geo STRING(MAX),
        normalized_location STRING(MAX),
        start_year INT64,
        start_month INT64,
        end_year INT64,
        end_month INT64,
    ) PRIMARY KEY(employment_id)""",

    """CREATE TABLE Expert (
        expert_profile_id STRING(36) NOT NULL,
        expert_first_name STRING(MAX),
        expert_last_name STRING(MAX),
        expert_country STRING(MAX),
        expert_linkedin STRING(MAX),
        expert_currency STRING(MAX),
        expert_rate INT64,
    ) PRIMARY KEY(expert_profile_id)""",

    """CREATE TABLE ExpertBio (
        expert_profile_id STRING(36) NOT NULL,
        bio_text STRING(MAX),
    ) PRIMARY KEY(expert_profile_id)""",

    """CREATE TABLE HasAngle (
        project_id STRING(36) NOT NULL,
        angle_id STRING(36) NOT NULL,
    ) PRIMARY KEY(project_id, angle_id)""",

    """CREATE TABLE InsightFor (
        insight_id STRING(36) NOT NULL,
        project_id STRING(36) NOT NULL,
    ) PRIMARY KEY(insight_id, project_id)""",

    """CREATE TABLE Participant (
        call_id STRING(36) NOT NULL,
        expert_profile_id STRING(36) NOT NULL,
    ) PRIMARY KEY(call_id, expert_profile_id)""",

    """CREATE TABLE Project (
        project_id STRING(36) NOT NULL,
        project_name STRING(MAX),
        project_type STRING(MAX),
        project_status STRING(MAX),
        project_industry STRING(MAX),
    ) PRIMARY KEY(project_id)""",

    """CREATE TABLE ProjectBrief (
        project_id STRING(36) NOT NULL,
        project_type STRING(MAX),
        brief STRING(MAX),
        software_focus BOOL,
    ) PRIMARY KEY(project_id)""",

    """CREATE TABLE ProjectHasDnc (
        project_id STRING(36) NOT NULL,
        dnc_setting_id STRING(36) NOT NULL,
    ) PRIMARY KEY(project_id, dnc_setting_id)""",

    """CREATE TABLE QualExpert (
        qualification_id STRING(36) NOT NULL,
        expert_profile_id STRING(36) NOT NULL,
    ) PRIMARY KEY(qualification_id, expert_profile_id)""",

    """CREATE TABLE QualProject (
        qualification_id STRING(36) NOT NULL,
        project_id STRING(36) NOT NULL,
    ) PRIMARY KEY(qualification_id, project_id)""",

    """CREATE TABLE Qualification (
        qualification_id STRING(36) NOT NULL,
        expert_profile_id STRING(36),
        project_id STRING(36),
        angle_id STRING(36),
        qualification_status STRING(MAX),
        qualification_type STRING(MAX),
        qualification_source STRING(MAX),
        relevance STRING(MAX),
        priority STRING(MAX),
        is_screened BOOL,
        approved_at STRING(MAX),
        rejected_by_id STRING(36),
        status_before_reject STRING(MAX),
        relevant_employment_id STRING(36),
        project_bio STRING(MAX),
        created_at STRING(MAX),
    ) PRIMARY KEY(qualification_id)""",

    """CREATE TABLE QualifiedFor (
        expert_profile_id STRING(36) NOT NULL,
        project_id STRING(36) NOT NULL,
    ) PRIMARY KEY(expert_profile_id, project_id)""",

    """CREATE TABLE RecordOf (
        transcript_id STRING(36) NOT NULL,
        call_id STRING(36) NOT NULL,
    ) PRIMARY KEY(transcript_id, call_id)""",

    """CREATE TABLE ScreenedBy (
        answer_id STRING(36) NOT NULL,
        expert_profile_id STRING(36) NOT NULL,
    ) PRIMARY KEY(answer_id, expert_profile_id)""",

    """CREATE TABLE ScreeningQA (
        answer_id STRING(36) NOT NULL,
        question STRING(MAX),
        answer STRING(MAX),
        answer_index INT64,
        qualification_id STRING(36),
        expert_profile_id STRING(36),
        project_id STRING(36),
        screening_method STRING(MAX),
    ) PRIMARY KEY(answer_id)""",

    """CREATE TABLE TranscriptDoc (
        transcript_id STRING(36) NOT NULL,
        transcript_status STRING(MAX),
        accent_code STRING(MAX),
        transcript_sent_at STRING(MAX),
        call_datetime STRING(MAX),
        call_duration INT64,
        call_id STRING(36),
        recording_file STRING(MAX),
    ) PRIMARY KEY(transcript_id)""",
]

# Indexes and FK constraints — applied AFTER data load
DDL_POST_LOAD = [
    "CREATE INDEX idx_aiinsight_project ON AIInsight(project_id)",
    "CREATE INDEX idx_angle_project ON Angle(project_id)",
    "CREATE INDEX idx_company_name ON Company(company_normalized_name)",
    "CREATE INDEX idx_dncsetting_project ON DncSetting(project_id)",
    "CREATE INDEX idx_employedat_company ON EmployedAt(company_id)",
    "CREATE INDEX idx_employedat_expert ON EmployedAt(expert_profile_id)",
    "CREATE INDEX idx_participant_expert ON Participant(expert_profile_id)",
    "CREATE INDEX idx_project_industry ON Project(project_industry)",
    "CREATE INDEX idx_qualexpert_expert ON QualExpert(expert_profile_id)",
    "CREATE INDEX idx_qualproject_project ON QualProject(project_id)",
    "CREATE INDEX idx_qual_expert ON Qualification(expert_profile_id)",
    "CREATE INDEX idx_qual_project ON Qualification(project_id)",
    "CREATE INDEX idx_qualifiedfor_project ON QualifiedFor(project_id)",
    "CREATE INDEX idx_screenedby_expert ON ScreenedBy(expert_profile_id)",
    "CREATE INDEX idx_screeningqa_expert ON ScreeningQA(expert_profile_id)",
    "CREATE INDEX idx_screeningqa_project ON ScreeningQA(project_id)",
]

# Tables in dependency order with columns.
# "large" flag = use smaller batch size.
TABLES = [
    # ── Node tables ─────────────────────────────────────────────────
    ("Expert", [
        "expert_profile_id", "expert_first_name", "expert_last_name",
        "expert_country", "expert_linkedin", "expert_currency", "expert_rate",
    ], False),
    ("ExpertBio", [
        "expert_profile_id", "bio_text",
    ], False),
    ("Company", [
        "company_id", "company_display_name", "company_normalized_name",
        "company_linkedin",
    ], True),  # 105K
    ("Project", [
        "project_id", "project_name", "project_type", "project_status",
        "project_industry",
    ], False),
    ("ProjectBrief", [
        "project_id", "project_type", "brief", "software_focus",
    ], False),
    ("Angle", [
        "angle_id", "project_id", "angle_name", "angle_type", "seniority",
        "tenure", "description", "location_ids", "derived_scope",
        "expert_keywords",
    ], False),
    ("CallRecord", [
        "call_id", "call_datetime", "call_duration", "call_status",
        "recording_file",
    ], False),
    ("DncSetting", [
        "dnc_setting_id", "project_id", "tenure", "recency_months",
        "setting_index",
    ], False),
    ("AIInsight", [
        "insight_id", "project_id", "insight_type", "insight_text",
    ], False),
    ("Qualification", [
        "qualification_id", "expert_profile_id", "project_id", "angle_id",
        "qualification_status", "qualification_type", "qualification_source",
        "relevance", "priority", "is_screened", "approved_at",
        "rejected_by_id", "status_before_reject", "relevant_employment_id",
        "project_bio", "created_at",
    ], True),  # 361K
    ("ScreeningQA", [
        "answer_id", "question", "answer", "answer_index",
        "qualification_id", "expert_profile_id", "project_id",
        "screening_method",
    ], True),  # 431K
    ("TranscriptDoc", [
        "transcript_id", "transcript_status", "accent_code",
        "transcript_sent_at", "call_datetime", "call_duration", "call_id",
        "recording_file",
    ], False),

    # ── Edge tables ─────────────────────────────────────────────────
    ("EmployedAt", [
        "employment_id", "expert_profile_id", "company_id", "position",
        "role", "seniority", "geo", "normalized_location", "start_year",
        "start_month", "end_year", "end_month",
    ], True),  # 59K
    ("AboutProject", ["transcript_id", "project_id"], False),
    ("AngleCompetitor", ["angle_id", "company_id"], False),
    ("AngleHasCompany", ["angle_id", "company_id"], False),
    ("CommissionedBy", ["call_id", "project_id"], False),
    ("DescribesBrief", ["project_id", "dest_project_id"], False),
    ("DescribesExpert", ["expert_profile_id"], False),
    ("DncBlocksCompany", ["dnc_setting_id", "company_id"], True),  # 100K
    ("HasAngle", ["project_id", "angle_id"], False),
    ("InsightFor", ["insight_id", "project_id"], False),
    ("Participant", ["call_id", "expert_profile_id"], False),
    ("ProjectHasDnc", ["project_id", "dnc_setting_id"], False),
    ("QualExpert", ["qualification_id", "expert_profile_id"], True),  # 361K
    ("QualProject", ["qualification_id", "project_id"], True),  # 361K
    ("QualifiedFor", ["expert_profile_id", "project_id"], True),  # 88K
    ("RecordOf", ["transcript_id", "call_id"], False),
    ("ScreenedBy", ["answer_id", "expert_profile_id"], True),  # 431K
]


def copy_table(src_db, dst_db, table_name: str, columns: list[str], large: bool) -> int:
    """Read all rows from source and write to destination in batches."""
    batch_size = BATCH_SIZE_LARGE if large else BATCH_SIZE_DEFAULT

    with src_db.snapshot() as snapshot:
        results = snapshot.read(
            table=table_name,
            columns=columns,
            keyset=KeySet(all_=True),
        )
        rows = list(results)

    if not rows:
        print(f"  {table_name}: 0 rows (empty)")
        return 0

    total = len(rows)
    written = 0
    for i in range(0, total, batch_size):
        batch = rows[i : i + batch_size]
        with dst_db.batch() as b:
            b.insert_or_update(
                table=table_name,
                columns=columns,
                values=batch,
            )
        written += len(batch)
        # Progress for large tables
        if large and written % 50000 < batch_size:
            print(f"    ... {written:,}/{total:,}")

    print(f"  {table_name}: {written:,} rows")
    return written


def main():
    src_client = spanner.Client(project=SRC_PROJECT)
    dst_client = spanner.Client(project=DST_PROJECT)

    src_instance = src_client.instance(SRC_INSTANCE)
    dst_instance = dst_client.instance(DST_INSTANCE)

    src_db = src_instance.database(SRC_DATABASE)

    # ── Step 1: Create database ─────────────────────────────────────
    print(f"Creating database '{DST_DATABASE}' in {DST_PROJECT}/{DST_INSTANCE}...")
    dst_db_handle = dst_instance.database(DST_DATABASE, ddl_statements=DDL_TABLES)
    try:
        operation = dst_db_handle.create()
        operation.result(timeout=600)
        print(f"  ✅ Database created with {len(DDL_TABLES)} tables")
    except Exception as e:
        if "already exists" in str(e).lower() or "ALREADY_EXISTS" in str(e):
            print(f"  ⏭️  Database already exists")
        else:
            raise

    dst_db = dst_instance.database(DST_DATABASE)

    # ── Step 2: Copy data ───────────────────────────────────────────
    print(f"\nCopying data from {SRC_PROJECT}/{SRC_DATABASE}...")
    total_rows = 0
    start = time.time()

    for table_name, columns, large in TABLES:
        try:
            count = copy_table(src_db, dst_db, table_name, columns, large)
            total_rows += count
        except Exception as e:
            print(f"  ❌ {table_name}: FAILED — {e}")
            raise

    elapsed = time.time() - start
    print(f"\n✅ Data copy done: {len(TABLES)} tables, {total_rows:,} rows in {elapsed:.1f}s")

    # ── Step 3: Create indexes ──────────────────────────────────────
    print(f"\nCreating {len(DDL_POST_LOAD)} indexes...")
    operation = dst_db.update_ddl(DDL_POST_LOAD)
    operation.result(timeout=600)
    print("  ✅ All indexes created")

    # ── Step 4: Apply Property Graph ────────────────────────────────
    print("\nApplying Property Graph 'ExpertNetworkV2'...")
    graph_ddl = """CREATE PROPERTY GRAPH ExpertNetworkV2
  NODE TABLES(
    AIInsight KEY(insight_id) LABEL AIInsight
      PROPERTIES(insight_id, insight_text, insight_type, project_id),
    Angle KEY(angle_id) LABEL Angle
      PROPERTIES(angle_id, angle_name, angle_type, derived_scope, description,
        expert_keywords, location_ids, project_id, seniority, tenure),
    CallRecord KEY(call_id) LABEL CallRecord
      PROPERTIES(call_datetime, call_duration, call_id, call_status, recording_file),
    Company KEY(company_id) LABEL Company
      PROPERTIES(company_display_name, company_id, company_linkedin, company_normalized_name),
    DncSetting KEY(dnc_setting_id) LABEL DncSetting
      PROPERTIES(dnc_setting_id, project_id, recency_months, setting_index, tenure),
    Expert KEY(expert_profile_id) LABEL Expert
      PROPERTIES(expert_country, expert_currency, expert_first_name, expert_last_name,
        expert_linkedin, expert_profile_id, expert_rate),
    ExpertBio KEY(expert_profile_id) LABEL ExpertBio
      PROPERTIES(bio_text, expert_profile_id),
    Project KEY(project_id) LABEL Project
      PROPERTIES(project_id, project_industry, project_name, project_status, project_type),
    ProjectBrief KEY(project_id) LABEL ProjectBrief
      PROPERTIES(brief, project_id, project_type, software_focus),
    Qualification KEY(qualification_id) LABEL Qualification
      PROPERTIES(angle_id, approved_at, created_at, expert_profile_id, is_screened,
        priority, project_bio, project_id, qualification_id, qualification_source,
        qualification_status, qualification_type, rejected_by_id, relevance,
        relevant_employment_id, status_before_reject),
    ScreeningQA KEY(answer_id) LABEL ScreeningQA
      PROPERTIES(answer, answer_id, answer_index, expert_profile_id, project_id,
        qualification_id, question, screening_method),
    TranscriptDoc KEY(transcript_id) LABEL TranscriptDoc
      PROPERTIES(accent_code, call_datetime, call_duration, call_id, recording_file,
        transcript_id, transcript_sent_at, transcript_status)
  )
  EDGE TABLES(
    AboutProject KEY(transcript_id, project_id)
      SOURCE KEY(transcript_id) REFERENCES TranscriptDoc(transcript_id)
      DESTINATION KEY(project_id) REFERENCES Project(project_id)
      LABEL AboutProject PROPERTIES(project_id, transcript_id),
    AngleCompetitor KEY(angle_id, company_id)
      SOURCE KEY(angle_id) REFERENCES Angle(angle_id)
      DESTINATION KEY(company_id) REFERENCES Company(company_id)
      LABEL AngleCompetitor PROPERTIES(angle_id, company_id),
    AngleHasCompany KEY(angle_id, company_id)
      SOURCE KEY(angle_id) REFERENCES Angle(angle_id)
      DESTINATION KEY(company_id) REFERENCES Company(company_id)
      LABEL AngleHasCompany PROPERTIES(angle_id, company_id),
    CommissionedBy KEY(call_id, project_id)
      SOURCE KEY(call_id) REFERENCES CallRecord(call_id)
      DESTINATION KEY(project_id) REFERENCES ProjectBrief(project_id)
      LABEL CommissionedBy PROPERTIES(call_id, project_id),
    DescribesBrief KEY(project_id, dest_project_id)
      SOURCE KEY(project_id) REFERENCES ProjectBrief(project_id)
      DESTINATION KEY(dest_project_id) REFERENCES Project(project_id)
      LABEL DescribesBrief PROPERTIES(dest_project_id, project_id),
    DescribesExpert KEY(expert_profile_id)
      SOURCE KEY(expert_profile_id) REFERENCES ExpertBio(expert_profile_id)
      DESTINATION KEY(expert_profile_id) REFERENCES Expert(expert_profile_id)
      LABEL DescribesExpert PROPERTIES(expert_profile_id),
    DncBlocksCompany KEY(dnc_setting_id, company_id)
      SOURCE KEY(dnc_setting_id) REFERENCES DncSetting(dnc_setting_id)
      DESTINATION KEY(company_id) REFERENCES Company(company_id)
      LABEL DncBlocksCompany PROPERTIES(company_id, dnc_setting_id),
    EmployedAt KEY(employment_id)
      SOURCE KEY(expert_profile_id) REFERENCES Expert(expert_profile_id)
      DESTINATION KEY(company_id) REFERENCES Company(company_id)
      LABEL EmployedAt PROPERTIES(company_id, employment_id, end_month, end_year,
        expert_profile_id, geo, normalized_location, position, role, seniority,
        start_month, start_year),
    HasAngle KEY(project_id, angle_id)
      SOURCE KEY(project_id) REFERENCES Project(project_id)
      DESTINATION KEY(angle_id) REFERENCES Angle(angle_id)
      LABEL HasAngle PROPERTIES(angle_id, project_id),
    InsightFor KEY(insight_id, project_id)
      SOURCE KEY(insight_id) REFERENCES AIInsight(insight_id)
      DESTINATION KEY(project_id) REFERENCES Project(project_id)
      LABEL InsightFor PROPERTIES(insight_id, project_id),
    Participant KEY(call_id, expert_profile_id)
      SOURCE KEY(call_id) REFERENCES CallRecord(call_id)
      DESTINATION KEY(expert_profile_id) REFERENCES Expert(expert_profile_id)
      LABEL Participant PROPERTIES(call_id, expert_profile_id),
    ProjectHasDnc KEY(project_id, dnc_setting_id)
      SOURCE KEY(project_id) REFERENCES Project(project_id)
      DESTINATION KEY(dnc_setting_id) REFERENCES DncSetting(dnc_setting_id)
      LABEL ProjectHasDnc PROPERTIES(dnc_setting_id, project_id),
    QualExpert KEY(qualification_id, expert_profile_id)
      SOURCE KEY(qualification_id) REFERENCES Qualification(qualification_id)
      DESTINATION KEY(expert_profile_id) REFERENCES Expert(expert_profile_id)
      LABEL QualExpert PROPERTIES(expert_profile_id, qualification_id),
    QualifiedFor KEY(expert_profile_id, project_id)
      SOURCE KEY(expert_profile_id) REFERENCES Expert(expert_profile_id)
      DESTINATION KEY(project_id) REFERENCES Project(project_id)
      LABEL QualifiedFor PROPERTIES(expert_profile_id, project_id),
    QualProject KEY(qualification_id, project_id)
      SOURCE KEY(qualification_id) REFERENCES Qualification(qualification_id)
      DESTINATION KEY(project_id) REFERENCES Project(project_id)
      LABEL QualProject PROPERTIES(project_id, qualification_id),
    RecordOf KEY(transcript_id, call_id)
      SOURCE KEY(transcript_id) REFERENCES TranscriptDoc(transcript_id)
      DESTINATION KEY(call_id) REFERENCES CallRecord(call_id)
      LABEL RecordOf PROPERTIES(call_id, transcript_id),
    ScreenedBy KEY(answer_id, expert_profile_id)
      SOURCE KEY(answer_id) REFERENCES ScreeningQA(answer_id)
      DESTINATION KEY(expert_profile_id) REFERENCES Expert(expert_profile_id)
      LABEL ScreenedBy PROPERTIES(answer_id, expert_profile_id)
  )"""

    operation = dst_db.update_ddl([graph_ddl])
    operation.result(timeout=600)
    print("  ✅ Property Graph 'ExpertNetworkV2' applied")

    print("\n" + "=" * 60)
    print("✅ kg_v2_3 fully copied to kg_v2_3_dev!")
    print("=" * 60)


if __name__ == "__main__":
    main()
