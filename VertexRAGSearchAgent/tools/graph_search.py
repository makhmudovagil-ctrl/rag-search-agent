"""
Graph Search Tool — structural expert search via Spanner Property Graph (GQL).

Queries two databases:
  - kg_products_v4_dev  → kg_graph      (products, industries, companies, roles)
  - kg_v2_3_dev         → ExpertNetworkV2 (qualifications, screening history)
"""

import os
import logging
from typing import Optional

import google.auth
from google.cloud import spanner
from google.cloud.spanner_v1.database import Database as SpannerDatabase

logger = logging.getLogger(__name__)

# ── Connection config ──────────────────────────────────────────────────────────
_PROJECT = os.getenv("GCP_PROJECT_ID", "gcp-poc-488614")
_INSTANCE = os.getenv("SPANNER_INSTANCE_ID", "kg-dev-instance")
_KG_DB = os.getenv("SPANNER_DATABASE_ID", "kg_products_v4_dev")
_OPS_DB = os.getenv("SPANNER_OPS_DATABASE_ID", "kg_v2_3_dev")
_DEFAULT_LIMIT = 20

_client: Optional[spanner.Client] = None
_kg_db: Optional[SpannerDatabase] = None
_ops_db: Optional[SpannerDatabase] = None


def _get_kg_db() -> SpannerDatabase:
    global _client, _kg_db
    if _kg_db is None:
        creds, _ = google.auth.default()
        _client = spanner.Client(project=_PROJECT, credentials=creds)
        _kg_db = _client.instance(_INSTANCE).database(_KG_DB)
    return _kg_db


def _get_ops_db() -> SpannerDatabase:
    global _client, _ops_db
    if _ops_db is None:
        creds, _ = google.auth.default()
        if _client is None:
            _client = spanner.Client(project=_PROJECT, credentials=creds)
        _ops_db = _client.instance(_INSTANCE).database(_OPS_DB)
    return _ops_db


def _run_gql(db: SpannerDatabase, sql: str, params: dict = None, types: dict = None) -> list[dict]:
    """Execute a GQL or SQL query and return rows as list of dicts."""
    with db.snapshot() as snapshot:
        result = snapshot.execute_sql(
            sql,
            params=params or {},
            param_types=types or {},
        )
        # Consume all rows first — metadata (fields) is populated only after iteration
        rows = list(result)
        fields = [field.name for field in result.fields]
        return [dict(zip(fields, row)) for row in rows]


# ── Tool functions ─────────────────────────────────────────────────────────────

def search_experts_by_product(
    product_name: str,
    supply_chain_position: Optional[str] = None,
    is_current_role: Optional[bool] = None,
    limit: int = _DEFAULT_LIMIT,
) -> dict:
    """Search experts who have hands-on experience with a specific product.

    Traversal path: Expert → EmploymentRecord → [INVOLVED_WITH] → Product

    Args:
        product_name: Product name to search for (case-insensitive substring match).
        supply_chain_position: Optional filter — one of: buyer, seller, user,
            evaluator, advisor, analyst, operator, competitor, neutral, none.
        is_current_role: If True, only current positions; if False, past only.
        limit: Max number of results to return.

    Returns:
        dict with keys: results (list), count (int), query_type (str).
    """
    filters = "LOWER(p.product_name) LIKE LOWER(@product_name)"
    params: dict = {"product_name": f"%{product_name}%"}
    param_types = {"product_name": spanner.param_types.STRING}

    if supply_chain_position:
        filters += " AND iw.supply_chain_position = @scp"
        params["scp"] = supply_chain_position
        param_types["scp"] = spanner.param_types.STRING

    if is_current_role is not None:
        filters += " AND er.is_current = @is_current"
        params["is_current"] = is_current_role
        param_types["is_current"] = spanner.param_types.BOOL

    sql = f"""
        GRAPH kg_graph
        MATCH (e:Expert)-[:HAS_EMPLOYMENT]->(er:EmploymentRecord)-[iw:INVOLVED_WITH]->(p:Product)
        WHERE {filters}
        RETURN DISTINCT
            e.expert_id,
            e.expert_name,
            p.product_name,
            iw.supply_chain_position,
            er.jobtitle_raw,
            er.is_current,
            er.start_year,
            er.end_year
        LIMIT @limit
    """
    params["limit"] = limit
    param_types["limit"] = spanner.param_types.INT64

    try:
        rows = _run_gql(_get_kg_db(), sql, params, param_types)
        return {"query_type": "product", "count": len(rows), "results": rows}
    except Exception as e:
        logger.error("search_experts_by_product failed: %s", e)
        return {"query_type": "product", "count": 0, "results": [], "error": str(e)}


def search_experts_by_company(
    company_name: str,
    function: Optional[str] = None,
    is_current_role: Optional[bool] = None,
    limit: int = _DEFAULT_LIMIT,
) -> dict:
    """Search experts who worked at a specific company.

    Traversal path: Expert → EmploymentRecord → [AT_COMPANY] → Company

    Args:
        company_name: Company name (case-insensitive substring match).
        function: Optional role function filter — one of: Operations, Commercial,
            Strategy, Finance, Engineering, IT, R&D, Procurement, Legal, HR, C-Suite.
        is_current_role: Filter by current/past employment.
        limit: Max results.
    """
    filters = "LOWER(c.company_name) LIKE LOWER(@company_name)"
    params: dict = {"company_name": f"%{company_name}%"}
    param_types = {"company_name": spanner.param_types.STRING}

    # HAS_ROLE and AT_COMPANY both originate from EmploymentRecord — use comma to split paths
    extra_match = ""
    if function:
        extra_match = ",\n              (er)-[:HAS_ROLE]->(r:Role)"
        filters += " AND r.function = @function"
        params["function"] = function
        param_types["function"] = spanner.param_types.STRING

    if is_current_role is not None:
        filters += " AND er.is_current = @is_current"
        params["is_current"] = is_current_role
        param_types["is_current"] = spanner.param_types.BOOL

    return_cols = "e.expert_id, e.expert_name, c.company_name, er.jobtitle_raw, er.is_current, er.start_year, er.end_year"
    if function:
        return_cols += ", r.function, r.seniority"

    sql = f"""
        GRAPH kg_graph
        MATCH (e:Expert)-[:HAS_EMPLOYMENT]->(er:EmploymentRecord)-[:AT_COMPANY]->(c:Company){extra_match}
        WHERE {filters}
        RETURN DISTINCT {return_cols}
        LIMIT @limit
    """
    params["limit"] = limit
    param_types["limit"] = spanner.param_types.INT64

    try:
        rows = _run_gql(_get_kg_db(), sql, params, param_types)
        return {"query_type": "company", "count": len(rows), "results": rows}
    except Exception as e:
        logger.error("search_experts_by_company failed: %s", e)
        return {"query_type": "company", "count": 0, "results": [], "error": str(e)}


def search_experts_by_industry(
    industry_name: str,
    function: Optional[str] = None,
    is_current_role: Optional[bool] = None,
    limit: int = _DEFAULT_LIMIT,
) -> dict:
    """Search experts who worked at companies in a specific industry.

    Traversal path: Expert → EmploymentRecord → [AT_COMPANY] → Company → [IN_INDUSTRY] → Industry

    Args:
        industry_name: Industry name (case-insensitive substring match).
        function: Optional role function filter.
        is_current_role: Filter by current/past employment.
        limit: Max results.
    """
    filters = "LOWER(i.name) LIKE LOWER(@industry_name)"
    params: dict = {"industry_name": f"%{industry_name}%"}
    param_types = {"industry_name": spanner.param_types.STRING}

    extra_match = ""
    if function:
        extra_match = ",\n              (er)-[:HAS_ROLE]->(r:Role)"
        filters += " AND r.function = @function"
        params["function"] = function
        param_types["function"] = spanner.param_types.STRING

    if is_current_role is not None:
        filters += " AND er.is_current = @is_current"
        params["is_current"] = is_current_role
        param_types["is_current"] = spanner.param_types.BOOL

    return_cols = "e.expert_id, e.expert_name, c.company_name, i.name AS industry_name, er.jobtitle_raw, er.is_current"
    if function:
        return_cols += ", r.function, r.seniority"

    sql = f"""
        GRAPH kg_graph
        MATCH (e:Expert)-[:HAS_EMPLOYMENT]->(er:EmploymentRecord)-[:AT_COMPANY]->(c:Company)-[:IN_INDUSTRY]->(i:Industry){extra_match}
        WHERE {filters}
        RETURN DISTINCT {return_cols}
        LIMIT @limit
    """
    params["limit"] = limit
    param_types["limit"] = spanner.param_types.INT64

    try:
        rows = _run_gql(_get_kg_db(), sql, params, param_types)
        return {"query_type": "industry", "count": len(rows), "results": rows}
    except Exception as e:
        logger.error("search_experts_by_industry failed: %s", e)
        return {"query_type": "industry", "count": 0, "results": [], "error": str(e)}


def search_experts_by_function(
    function: str,
    seniority: Optional[str] = None,
    industry_name: Optional[str] = None,
    is_current_role: Optional[bool] = None,
    limit: int = _DEFAULT_LIMIT,
) -> dict:
    """Search experts by their role function (department/area).

    Traversal path: Expert → EmploymentRecord → [HAS_ROLE] → Role

    Args:
        function: Role function — one of: Operations, Commercial, Strategy, Finance,
            Engineering, IT, R&D, Procurement, Legal, HR, C-Suite.
        seniority: Optional seniority filter (e.g. Senior, C-Suite, Mid-Level, Junior).
        industry_name: Optional industry context filter.
        is_current_role: Filter by current/past role.
        limit: Max results.
    """
    filters = "r.function = @function"
    params: dict = {"function": function}
    param_types = {"function": spanner.param_types.STRING}

    if seniority:
        filters += " AND r.seniority = @seniority"
        params["seniority"] = seniority
        param_types["seniority"] = spanner.param_types.STRING

    if is_current_role is not None:
        filters += " AND er.is_current = @is_current"
        params["is_current"] = is_current_role
        param_types["is_current"] = spanner.param_types.BOOL

    industry_join = ""
    if industry_name:
        industry_join = "-[:AT_COMPANY]->(c:Company)-[:IN_INDUSTRY]->(i:Industry)"
        filters += " AND LOWER(i.name) LIKE LOWER(@industry_name)"
        params["industry_name"] = f"%{industry_name}%"
        param_types["industry_name"] = spanner.param_types.STRING

    return_cols = "e.expert_id, e.expert_name, r.function, r.seniority, er.jobtitle_raw, er.is_current"
    if industry_name:
        return_cols += ", c.company_name, i.name AS industry_name"

    sql = f"""
        GRAPH kg_graph
        MATCH (e:Expert)-[:HAS_EMPLOYMENT]->(er:EmploymentRecord)-[:HAS_ROLE]->(r:Role){industry_join}
        WHERE {filters}
        RETURN DISTINCT {return_cols}
        LIMIT @limit
    """
    params["limit"] = limit
    param_types["limit"] = spanner.param_types.INT64

    try:
        rows = _run_gql(_get_kg_db(), sql, params, param_types)
        return {"query_type": "function", "count": len(rows), "results": rows}
    except Exception as e:
        logger.error("search_experts_by_function failed: %s", e)
        return {"query_type": "function", "count": 0, "results": [], "error": str(e)}


def search_experts_by_keyword(
    keyword: str,
    limit: int = _DEFAULT_LIMIT,
) -> dict:
    """Search experts via keyword → knowledge artifact → expert path.

    Traversal path: Keyword → [MENTIONED_IN] → KnowledgeArtifact → [RELEVANT_EMPLOYMENT] → EmploymentRecord → Expert

    Args:
        keyword: Keyword text (case-insensitive substring match).
        limit: Max results.
    """
    sql = """
        GRAPH kg_graph
        MATCH (kw:Keyword)-[:MENTIONED_IN]->(ka:KnowledgeArtifact)-[:RELEVANT_EMPLOYMENT]->(er:EmploymentRecord)<-[:HAS_EMPLOYMENT]-(e:Expert)
        WHERE LOWER(kw.keyword) LIKE LOWER(@keyword)
        RETURN DISTINCT
            e.expert_id,
            e.expert_name,
            kw.keyword,
            ka.artifact_id,
            er.jobtitle_raw,
            er.is_current
        LIMIT @limit
    """
    params = {"keyword": f"%{keyword}%", "limit": limit}
    param_types = {
        "keyword": spanner.param_types.STRING,
        "limit": spanner.param_types.INT64,
    }

    try:
        rows = _run_gql(_get_kg_db(), sql, params, param_types)
        return {"query_type": "keyword", "count": len(rows), "results": rows}
    except Exception as e:
        logger.error("search_experts_by_keyword failed: %s", e)
        return {"query_type": "keyword", "count": 0, "results": [], "error": str(e)}


def search_experts_multi_hop(
    product_name: Optional[str] = None,
    industry_name: Optional[str] = None,
    function: Optional[str] = None,
    company_name: Optional[str] = None,
    supply_chain_position: Optional[str] = None,
    is_current_role: Optional[bool] = None,
    limit: int = _DEFAULT_LIMIT,
) -> dict:
    """Multi-hop expert search combining product, industry, and function constraints.

    Builds a single GQL query that traverses multiple relationship types.
    Use when the brief has two or more structural constraints simultaneously
    (e.g. 'buyers of Salesforce in Financial Services').

    Args:
        product_name: Optional product constraint.
        industry_name: Optional industry constraint.
        function: Optional role function constraint.
        company_name: Optional company name constraint.
        supply_chain_position: Optional supply chain position filter.
        is_current_role: Filter by current/past employment.
        limit: Max results.
    """
    if not any([product_name, industry_name, function, company_name]):
        return {
            "query_type": "multi_hop",
            "count": 0,
            "results": [],
            "error": "At least one search constraint must be provided.",
        }

    # Each relationship from EmploymentRecord is a separate path in GQL.
    # Base path: Expert → EmploymentRecord
    # Branch 1: er -[INVOLVED_WITH]-> Product
    # Branch 2: er -[AT_COMPANY]-> Company -[IN_INDUSTRY]-> Industry
    # Branch 3: er -[HAS_ROLE]-> Role
    # All branches share the same er node via comma-separated MATCH patterns.

    where_parts: list[str] = []
    params: dict = {}
    param_types: dict = {}
    extra_paths: list[str] = []

    if product_name:
        extra_paths.append("(er)-[iw:INVOLVED_WITH]->(p:Product)")
        where_parts.append("LOWER(p.product_name) LIKE LOWER(@product_name)")
        params["product_name"] = f"%{product_name}%"
        param_types["product_name"] = spanner.param_types.STRING

    if supply_chain_position and product_name:
        where_parts.append("iw.supply_chain_position = @scp")
        params["scp"] = supply_chain_position
        param_types["scp"] = spanner.param_types.STRING

    if company_name or industry_name:
        if industry_name:
            extra_paths.append("(er)-[:AT_COMPANY]->(c:Company)-[:IN_INDUSTRY]->(i:Industry)")
            where_parts.append("LOWER(i.name) LIKE LOWER(@industry_name)")
            params["industry_name"] = f"%{industry_name}%"
            param_types["industry_name"] = spanner.param_types.STRING
        else:
            extra_paths.append("(er)-[:AT_COMPANY]->(c:Company)")

        if company_name:
            where_parts.append("LOWER(c.company_name) LIKE LOWER(@company_name)")
            params["company_name"] = f"%{company_name}%"
            param_types["company_name"] = spanner.param_types.STRING

    if function:
        extra_paths.append("(er)-[:HAS_ROLE]->(r:Role)")
        where_parts.append("r.function = @function")
        params["function"] = function
        param_types["function"] = spanner.param_types.STRING

    if is_current_role is not None:
        where_parts.append("er.is_current = @is_current")
        params["is_current"] = is_current_role
        param_types["is_current"] = spanner.param_types.BOOL

    return_cols = ["e.expert_id", "e.expert_name", "er.jobtitle_raw", "er.is_current"]
    if product_name:
        return_cols += ["p.product_name", "iw.supply_chain_position"]
    if company_name or industry_name:
        return_cols.append("c.company_name")
    if industry_name:
        return_cols.append("i.name AS industry_name")
    if function:
        return_cols += ["r.function", "r.seniority"]

    base_path = "(e:Expert)-[:HAS_EMPLOYMENT]->(er:EmploymentRecord)"
    all_paths = ", ".join([base_path] + extra_paths)
    where_clause = " AND ".join(where_parts)
    return_clause = ", ".join(return_cols)

    sql = f"""
        GRAPH kg_graph
        MATCH {all_paths}
        WHERE {where_clause}
        RETURN DISTINCT {return_clause}
        LIMIT @limit
    """
    params["limit"] = limit
    param_types["limit"] = spanner.param_types.INT64

    try:
        rows = _run_gql(_get_kg_db(), sql, params, param_types)
        return {"query_type": "multi_hop", "count": len(rows), "results": rows}
    except Exception as e:
        logger.error("search_experts_multi_hop failed: %s", e)
        return {"query_type": "multi_hop", "count": 0, "results": [], "error": str(e)}


def get_expert_profile(expert_id: str) -> dict:
    """Fetch full profile for a specific expert including all employment history.

    Args:
        expert_id: The expert's UUID from the Knowledge Graph.
    """
    sql = """
        GRAPH kg_graph
        MATCH (e:Expert)-[:HAS_EMPLOYMENT]->(er:EmploymentRecord)-[:AT_COMPANY]->(c:Company),
              (er)-[:HAS_ROLE]->(r:Role)
        WHERE e.expert_id = @expert_id
        RETURN
            e.expert_id, e.expert_name, e.is_active, e.skills,
            c.company_name, c.company_type,
            er.jobtitle_raw, er.position,
            r.function, r.seniority,
            er.start_year, er.end_year, er.is_current,
            er.geo, er.responsibilities
        ORDER BY er.is_current DESC, er.start_year DESC
        LIMIT 20
    """
    try:
        rows = _run_gql(
            _get_kg_db(),
            sql,
            {"expert_id": expert_id},
            {"expert_id": spanner.param_types.STRING},
        )
        return {"expert_id": expert_id, "count": len(rows), "employment_history": rows}
    except Exception as e:
        logger.error("get_expert_profile failed: %s", e)
        return {"expert_id": expert_id, "count": 0, "employment_history": [], "error": str(e)}


def get_coverage_diagnostics(
    product_name: Optional[str] = None,
    company_name: Optional[str] = None,
) -> dict:
    """Explain why search results may be sparse.

    Checks whether the entity exists at all vs. having no linked experts.
    Uses pre-computed expert_count / artifact_count columns.

    Args:
        product_name: Product to diagnose.
        company_name: Company to diagnose.
    """
    diagnostics = {}

    if product_name:
        with _get_kg_db().snapshot() as s:
            rows = list(s.execute_sql(
                "SELECT product_name, artifact_count FROM product "
                "WHERE LOWER(product_name) LIKE LOWER(@name) LIMIT 5",
                params={"name": f"%{product_name}%"},
                param_types={"name": spanner.param_types.STRING},
            ))
        if not rows:
            diagnostics["product"] = {"status": "not_found", "name": product_name}
        else:
            diagnostics["product"] = {
                "status": "found",
                "matches": [{"name": r[0], "artifact_count": r[1]} for r in rows],
            }

    if company_name:
        with _get_kg_db().snapshot() as s:
            rows = list(s.execute_sql(
                "SELECT company_name, expert_count, ambiguity_flag FROM company "
                "WHERE LOWER(company_name) LIKE LOWER(@name) LIMIT 5",
                params={"name": f"%{company_name}%"},
                param_types={"name": spanner.param_types.STRING},
            ))
        if not rows:
            diagnostics["company"] = {"status": "not_found", "name": company_name}
        else:
            diagnostics["company"] = {
                "status": "found",
                "matches": [
                    {"name": r[0], "expert_count": r[1], "ambiguity_flag": r[2]}
                    for r in rows
                ],
            }

    return diagnostics


def check_company_disambiguation(company_name: str) -> dict:
    """Check if a company name is ambiguous and return aliases.

    Queries the company table for matches, checks ambiguity_flag, and fetches
    any known aliases from company_alias. Used by Scout to surface
    disambiguation notices when company names are ambiguous.

    Args:
        company_name: Company name to check (case-insensitive substring match).

    Returns:
        Dict with 'status' key:
        - "not_found": no matching company in database
        - "unambiguous": exactly one match, not flagged
        - "ambiguous": multiple matches or ambiguity_flag is true
        - "error": query failed
    """
    try:
        db = _get_kg_db()

        # Query 1: find matching companies
        with db.snapshot() as snapshot:
            company_rows = list(snapshot.execute_sql(
                "SELECT company_id, company_name, expert_count, ambiguity_flag "
                "FROM company "
                "WHERE LOWER(company_name) LIKE LOWER(@name) LIMIT 10",
                params={"name": f"%{company_name}%"},
                param_types={"name": spanner.param_types.STRING},
            ))

        if not company_rows:
            return {"status": "not_found", "name": company_name}

        matches = [
            {
                "company_id": r[0],
                "company_name": r[1],
                "expert_count": r[2],
                "ambiguity_flag": r[3],
            }
            for r in company_rows
        ]

        # Query 2: fetch aliases for ALL matched company_ids
        company_ids = [r[0] for r in company_rows]
        aliases = []
        with db.snapshot() as snapshot:
            alias_rows = list(snapshot.execute_sql(
                "SELECT alias_id, alias_name, alias_type, company_id "
                "FROM company_alias "
                "WHERE company_id IN UNNEST(@ids)",
                params={"ids": company_ids},
                param_types={"ids": spanner.param_types.Array(spanner.param_types.STRING)},
            ))
        aliases = [
            {
                "alias_name": r[1],
                "alias_type": r[2],
                "company_id": r[3],
            }
            for r in alias_rows
        ]

        # Determine ambiguity: flagged OR multiple distinct matches
        any_flagged = any(m.get("ambiguity_flag") for m in matches)
        is_ambiguous = any_flagged or len(matches) > 1

        if is_ambiguous:
            return {
                "status": "ambiguous",
                "name": company_name,
                "matches": matches,
                "aliases": aliases,
            }

        # Single unambiguous match
        result = {"status": "unambiguous", "company": matches[0]}
        if aliases:
            result["aliases"] = aliases
        return result

    except Exception as e:
        logger.error("check_company_disambiguation failed: %s", e)
        return {"status": "error", "error": str(e)}
