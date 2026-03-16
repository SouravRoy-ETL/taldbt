"""
Universal Input/Output Component Parser.
Works for tMysqlInput, tOracleInput, tSnowflakeInput, tDBInput,
tFileInputDelimited, tFileInputJSON, tRESTClient, etc.
Extracts connection info, embedded queries, and file paths.
"""
import re
from taldbt.models.ast_models import (
    ComponentAST, SourceInfo, SourceType, ConnectionInfo,
    EmbeddedQuery, DbtStrategy,
)


def _local_tag(elem) -> str:
    tag = elem.tag
    return tag.split("}")[1] if "}" in tag else tag


def _clean_param(val: str) -> str:
    """Strip surrounding quotes and Talend escaping."""
    v = val.strip()
    if v.startswith('"') and v.endswith('"'):
        v = v[1:-1]
    if v.startswith("'") and v.endswith("'"):
        v = v[1:-1]
    return v


def _clean_query(raw: str) -> tuple[str, list[str], bool, list[str]]:
    """Clean an embedded SQL query from Talend formatting."""
    sql = raw.strip()
    # Remove surrounding quotes
    if sql.startswith('"') and sql.endswith('"'):
        sql = sql[1:-1]
    # Talend HTML entities
    sql = sql.replace("&#10;", "\n").replace("&#13;", "").replace("&amp;", "&")
    sql = sql.replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", '"')
    # Remove backtick quoting (MySQL-specific)
    cleaned = sql.replace("`", "")

    # Find source tables (simple: FROM <table> or JOIN <table>)
    tables = re.findall(r"(?:FROM|JOIN)\s+(\w+)", cleaned, re.IGNORECASE)

    # Find context variable references
    ctx_refs = re.findall(r"context\.(\w+)", cleaned)
    has_ctx = len(ctx_refs) > 0

    return cleaned, list(set(tables)), has_ctx, ctx_refs


def _detect_source_type(comp_type: str) -> SourceType:
    """Detect source type from component name."""
    ct = comp_type.lower()
    if "fileinputdelimited" in ct or "fileinputcsv" in ct:
        return SourceType.FILE_CSV
    if "fileinputjson" in ct or "extractjson" in ct:
        return SourceType.FILE_JSON
    if "fileinputxml" in ct or "extractxml" in ct:
        return SourceType.FILE_XML
    if "fileinputexcel" in ct:
        return SourceType.FILE_EXCEL
    if "fileinputparquet" in ct:
        return SourceType.FILE_PARQUET
    if "rest" in ct or "http" in ct:
        return SourceType.API_REST
    if "soap" in ct or "bapi" in ct:
        return SourceType.API_SOAP
    if "salesforce" in ct or "netsuite" in ct or "marketo" in ct or "servicenow" in ct:
        return SourceType.SAAS
    # Default: database
    return SourceType.DATABASE


def parse_input_component(comp: ComponentAST, node_elem=None) -> None:
    """Enrich a ComponentAST with source/connection info."""
    p = comp.parameters
    source_type = _detect_source_type(comp.component_type)

    info = SourceInfo(source_type=source_type)

    # ── Database Components ──────────────────────────
    if source_type == SourceType.DATABASE:
        info.connection = ConnectionInfo(
            host=_clean_param(p.get("HOST", "")),
            port=_clean_param(p.get("PORT", "")),
            database=_clean_param(p.get("DBNAME", "")),
            schema_name=_clean_param(p.get("SCHEMA_DB", "")),
            table=_clean_param(p.get("TABLE", "")),
            db_type=p.get("DB_VERSION", "") or p.get("TYPE", ""),
            username=_clean_param(p.get("USER", "")),
            properties=_clean_param(p.get("PROPERTIES", "")),
        )

        # Embedded query
        raw_query = p.get("QUERY", "")
        if raw_query and len(raw_query) > 10:
            cleaned, tables, has_ctx, ctx_refs = _clean_query(raw_query)
            info.query = EmbeddedQuery(
                raw_sql=raw_query,
                cleaned_sql=cleaned,
                source_tables=tables,
                has_context_vars=has_ctx,
                context_var_refs=ctx_refs,
            )

        info.dbt_strategy = DbtStrategy.SOURCE

    # ── File Components ──────────────────────────────
    elif source_type in (SourceType.FILE_CSV, SourceType.FILE_JSON,
                         SourceType.FILE_XML, SourceType.FILE_EXCEL,
                         SourceType.FILE_PARQUET):
        info.file_path = _clean_param(p.get("FILENAME", ""))
        info.delimiter = _clean_param(p.get("FIELDSEPARATOR", ","))
        info.has_header = p.get("HEADER", "true").lower() == "true"
        info.encoding = _clean_param(p.get("ENCODING", "UTF-8"))
        info.dbt_strategy = DbtStrategy.SEED

    # ── API Components ───────────────────────────────
    elif source_type in (SourceType.API_REST, SourceType.API_SOAP):
        info.url = _clean_param(p.get("URL", ""))
        info.method = _clean_param(p.get("METHOD", "GET"))
        info.dbt_strategy = DbtStrategy.TEMPORAL_ACTIVITY

    # ── SaaS Components ──────────────────────────────
    elif source_type == SourceType.SAAS:
        info.url = _clean_param(p.get("ENDPOINT", "") or p.get("URL", ""))
        info.dbt_strategy = DbtStrategy.TEMPORAL_ACTIVITY

    # Extract columns from schemas
    if "FLOW" in comp.schemas:
        info.columns = comp.schemas["FLOW"]
    elif comp.schemas:
        # Take the first available schema
        info.columns = list(comp.schemas.values())[0]

    comp.source_info = info
    comp.confidence = 1.0 if source_type == SourceType.DATABASE else 0.9
