"""
Migration Engine: takes a MigrationBrief -> generates complete, correct dbt SQL.
Uses sqlglot for universal dialect transpilation (17+ SQL dialects -> DuckDB).
"""
from __future__ import annotations
import re
from typing import Optional

try:
    import sqlglot
    from sqlglot import exp as sqlexp
    HAS_SQLGLOT = True
except ImportError:
    HAS_SQLGLOT = False

from taldbt.expert.job_analyzer import (
    MigrationBrief, SourceBrief, TransformBrief, SinkBrief,
)
from taldbt.llm.knowledge_base import translate_expression
from taldbt.models.ast_models import ExpressionStrategy

_JAVA_RESERVED = frozenset({
    'class', 'order', 'group', 'default', 'table', 'column', 'index',
    'key', 'type', 'value', 'name', 'select', 'where', 'from', 'join',
    'union', 'insert', 'update', 'delete', 'create', 'drop', 'alter',
    'null', 'true', 'false', 'int', 'long', 'float', 'double', 'boolean',
    'char', 'string', 'byte', 'short', 'void', 'return', 'new', 'this',
    'super', 'abstract', 'final', 'static', 'public', 'private', 'protected',
})

_SQLGLOT_DIALECTS = {
    "mysql": "mysql", "tsql": "tsql", "mssql": "tsql",
    "oracle": "oracle", "postgres": "postgres", "postgresql": "postgres",
    "bigquery": "bigquery", "redshift": "redshift", "snowflake": "snowflake",
    "teradata": "teradata", "hive": "hive", "sqlite": "sqlite",
    "databricks": "databricks", "spark": "spark", "presto": "presto",
    "trino": "trino", "clickhouse": "clickhouse",
}

_current_dialect = "mysql"


def _reconstruct_sql(raw, dialect="mysql"):
    sql = raw.strip().strip('"').strip("'")
    sql = sql.replace("&#10;", "\n").replace("&#13;", "").replace("&#9;", " ")
    sql = sql.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    sql = sql.replace("\\n", "\n").replace("\\t", " ").replace("`", "")
    sql = re.sub(r'"\s*\+\s*context\.(\w+)\s*\+\s*"', r"'{{ var('\1', '') }}'", sql)
    sql = re.sub(r"context\.getProperty\(['\"](\w+)['\"]\)", r"'{{ var('\1', '') }}'", sql)
    sql = re.sub(r"context\.(\w+)", r"'{{ var('\1', '') }}'", sql)
    if HAS_SQLGLOT:
        rd = _SQLGLOT_DIALECTS.get(dialect, "mysql")
        try:
            parsed = sqlglot.parse_one(sql, read=rd, error_level=sqlglot.ErrorLevel.IGNORE)
            for table in parsed.find_all(sqlexp.Table):
                if table.db: table.set("db", None)
                if table.catalog: table.set("catalog", None)
            result = parsed.sql(dialect="duckdb")
            result = re.sub(r'\b(\w+)\.(\w+)\.(\w+)\b', r'\2.\3', result)
            return result
        except Exception:
            pass
    sql = re.sub(r'\b(\w+)\.(\w+)\.(\w+)\b', r'\2.\3', sql)
    sql = re.sub(r'(FROM|JOIN)\s+(\w+)\.(\w+)', r'\1 \3', sql, flags=re.IGNORECASE)
    return sql.strip()


def _transpile_to_duckdb(expr, source_dialect="mysql"):
    """Universal sqlglot transpilation. Handles ALL dialect functions."""
    if not HAS_SQLGLOT or not expr or not expr.strip():
        return expr
    if '{{' in expr or '{%' in expr:
        return expr
    stripped = expr.strip()
    if re.match(r'^[\w]+\.[\w]+$', stripped):
        return expr
    if re.match(r'^\d+$', stripped):
        return expr
    if re.match(r"^'[^']*'$", stripped):
        return expr
    if '(' not in expr:
        return expr
    # Single-pass transpilation with the source dialect.
    # No fallback loop — mysql fallback was converting valid DuckDB STRFTIME
    # to DATE_FORMAT (reading DuckDB syntax as mysql, mangling it).
    rd = _SQLGLOT_DIALECTS.get(source_dialect, "mysql")
    try:
        parsed = sqlglot.parse_one(expr, read=rd, error_level=sqlglot.ErrorLevel.IGNORE)
        result = parsed.sql(dialect="duckdb")
        if result and len(result) > 0:
            return result
    except Exception:
        pass
    return expr


def _resolve_expr(raw_expr, flow_to_cte):
    if not raw_expr or not raw_expr.strip():
        return "NULL", "DETERMINISTIC"
    translated, strategy = translate_expression(raw_expr)
    ci_map = {k.lower(): v for k, v in flow_to_cte.items()}
    cte_names = set(flow_to_cte.values())
    def _replace(m):
        prefix, col = m.group(1), m.group(2)
        if prefix in cte_names:
            return m.group(0)
        mapped = ci_map.get(prefix.lower())
        if mapped:
            return f"{mapped}.{col}"
        if prefix.startswith(('t', 'T')) and any(c.isdigit() for c in prefix):
            return m.group(0)
        return m.group(0)
    resolved = re.sub(r'\b([a-zA-Z]\w+)\.(\w+)\b', _replace, translated)
    # Only run sqlglot on expressions that knowledge_base could NOT handle.
    # If KB already translated (DETERMINISTIC or KNOWLEDGE_BASE), the result
    # is already valid DuckDB SQL. Running sqlglot on it would UNDO the
    # translation (e.g., STRFTIME back to DATE_FORMAT).
    sval = strategy.value if hasattr(strategy, 'value') else str(strategy)
    if sval == 'LLM_REQUIRED':
        resolved = _transpile_to_duckdb(resolved, _current_dialect)
    return resolved, sval


def _check_unresolved(sql, flow_to_cte):
    known = set(flow_to_cte.values()) | {k.lower() for k in flow_to_cte}
    for m in re.finditer(r'\b([a-zA-Z]\w+)\.(\w+)\b', sql):
        p = m.group(1)
        if (p not in known and p.lower() not in known
            and not p.startswith(('t', 'T', 'read_', 'var', 'CURRENT', 'ROW_'))):
            return True
    return False


def _build_source_cte(src, dialect):
    name = src.cte_name
    if src.role == "data_source_db":
        sn = src.database_name or src.schema_name or 'raw'
        tbl = src.table_name or name
        ref = "{{ source('" + sn + "', '" + tbl + "') }}"
        return f"{name} AS (\n    SELECT * FROM {ref}\n)"
    elif src.role == "data_source_file":
        fmt = src.file_format
        path = src.file_path
        if fmt in ("csv", "excel", ""):
            return f"{name} AS (\n    SELECT * FROM read_csv('{path}', header=true, auto_detect=true)\n)"
        elif fmt == "json":
            return f"{name} AS (\n    SELECT * FROM read_json_auto('{path}')\n)"
        elif fmt == "parquet":
            return f"{name} AS (\n    SELECT * FROM read_parquet('{path}')\n)"
        elif fmt == "xml":
            return f"{name} AS (\n    /* TODO: XML source */\n    SELECT NULL AS _placeholder\n)"
        return f"{name} AS (\n    SELECT * FROM read_csv_auto('{path}')\n)"
    elif src.needs_temporal:
        return (f"{name} AS (\n    /* TODO: {src.component_type} -> Temporal activity */\n"
                f"    SELECT NULL AS _placeholder\n)")
    if src.table_name:
        return f"{name} AS (\n    SELECT * FROM {src.table_name}\n)"
    return f"{name} AS (\n    SELECT NULL AS _placeholder\n)"


def _build_tmap_cte(tx, flow_to_cte):
    name = tx.cte_name
    from_cte = tx.main_input or (tx.input_ctes[0] if tx.input_ctes else "dual")
    from_clause = f"    FROM {from_cte}"
    join_lines = []
    for j in tx.join_clauses:
        lcte = j["lookup_cte"]
        jtype = j["join_type"].upper()
        mode = j.get("matching_mode", "UNIQUE_MATCH")
        if jtype in ("INNER", ""): sql_join = "INNER JOIN"
        elif "LEFT" in jtype: sql_join = "LEFT JOIN"
        elif "CROSS" in jtype or mode == "ALL_ROWS": sql_join = "CROSS JOIN"
        else: sql_join = "INNER JOIN"
        if j["on_keys"] and sql_join != "CROSS JOIN":
            on_parts = []
            for k in j["on_keys"]:
                resolved, _ = _resolve_expr(k["expr"], flow_to_cte)
                on_parts.append(f"{lcte}.{k['lookup_col']} = {resolved}")
            join_lines.append(f"    {sql_join} {lcte} ON {' AND '.join(on_parts)}")
        else:
            join_lines.append(f"    CROSS JOIN {lcte}")
    select_cols = []
    for ei in tx.select_expressions:
        col_name = ei["column"]
        raw_expr = ei["expression"]
        if raw_expr and raw_expr.strip():
            resolved, strategy = _resolve_expr(raw_expr, flow_to_cte)
            for v in tx.var_expressions:
                vr, _ = _resolve_expr(v["expr"], flow_to_cte)
                resolved = re.sub(rf'\bVar\.{re.escape(v["name"])}\b', f"({vr})", resolved)
            if _check_unresolved(resolved, flow_to_cte):
                select_cols.append(f"    NULL AS {col_name} /* UNRESOLVED: {raw_expr.strip()[:50]} */")
            elif strategy == "LLM_REQUIRED":
                select_cols.append(f"    {resolved} AS {col_name} /* TODO [AI]: review */")
            elif resolved != col_name:
                select_cols.append(f"    {resolved} AS {col_name}")
            else:
                select_cols.append(f"    {resolved}")
        else:
            select_cols.append(f"    NULL AS {col_name}")
    if not select_cols:
        select_cols.append("    *")
    where_clause = ""
    if tx.where_filter:
        rf, _ = _resolve_expr(tx.where_filter, flow_to_cte)
        if _check_unresolved(rf, flow_to_cte):
            where_clause = f"\n    /* WHERE {rf} -- UNRESOLVED: stale reference */"
        else:
            where_clause = f"\n    WHERE {rf}"
    body = f"    SELECT\n{','.join(chr(10) + c for c in select_cols)}\n{from_clause}"
    if join_lines: body += "\n" + "\n".join(join_lines)
    if where_clause: body += where_clause
    return f"{name} AS (\n{body}\n)"


def _build_filter_cte(tx, flow_to_cte):
    ic = tx.input_ctes[0] if tx.input_ctes else "dual"
    if tx.filter_conditions:
        conds = []
        for fc in tx.filter_conditions:
            r, _ = _resolve_expr(fc, flow_to_cte)
            conds.append(r)
        return f"{tx.cte_name} AS (\n    SELECT * FROM {ic}\n    WHERE {' AND '.join(conds)}\n)"
    return f"{tx.cte_name} AS (\n    SELECT * FROM {ic}\n)"


def _build_aggregate_cte(tx):
    ic = tx.input_ctes[0] if tx.input_ctes else "dual"
    gc = ", ".join(f'"{g}"' for g in tx.group_by_cols) if tx.group_by_cols else ""
    ap = []
    for a in tx.aggregate_funcs:
        ap.append(f'{a["func"].upper()}("{a["input"]}") AS "{a["output"]}"')
    sel = ", ".join(([gc] if gc else []) + ap)
    gb = f"\n    GROUP BY {gc}" if gc else ""
    return f"{tx.cte_name} AS (\n    SELECT {sel}\n    FROM {ic}{gb}\n)"


def _build_sort_cte(tx):
    ic = tx.input_ctes[0] if tx.input_ctes else "dual"
    order = ", ".join(f'"{s["column"]}" {s["order"]}' for s in tx.sort_cols)
    return f"{tx.cte_name} AS (\n    SELECT * FROM {ic}\n    ORDER BY {order}\n)"


def _build_dedup_cte(tx):
    ic = tx.input_ctes[0] if tx.input_ctes else "dual"
    keys = ", ".join(f'"{k}"' for k in tx.dedup_keys)
    return (f"{tx.cte_name} AS (\n    SELECT * FROM (\n"
            f"        SELECT *, ROW_NUMBER() OVER (PARTITION BY {keys}) AS _rn\n"
            f"        FROM {ic}\n    ) WHERE _rn = 1\n)")


def _build_union_cte(tx):
    parts = "\n    UNION ALL\n    ".join(f"SELECT * FROM {c}" for c in tx.input_ctes)
    return f"{tx.cte_name} AS (\n    {parts}\n)"


def _build_custom_code_cte(tx, flow_to_cte, llm_fn=None):
    ic = tx.input_ctes[0] if tx.input_ctes else "dual"
    if llm_fn and tx.java_code:
        from taldbt.models.ast_models import ComponentAST, ComponentBehavior
        comp = ComponentAST(unique_name=tx.cte_name, component_type=tx.component_type,
                            behavior=ComponentBehavior.CUSTOM_CODE, java_code=tx.java_code)
        result = llm_fn(comp, tx.input_ctes, flow_to_cte)
        if result: return result
    cp = (tx.java_code or "no code")[:150].replace('\n', ' ')
    return (f"{tx.cte_name} AS (\n    /* TODO [AI]: {tx.component_type} requires LLM */\n"
            f"    /* Java: {cp} */\n    SELECT * FROM {ic}\n)")


def _build_passthrough_cte(name, input_cte):
    return f"{name} AS (\n    SELECT * FROM {input_cte}\n)"


def generate_model(brief, llm_fn=None):
    if not brief.sources and not brief.transforms:
        return None
    global _current_dialect
    _current_dialect = brief.primary_dialect or "mysql"
    ctes = []
    last_cte = None
    for src in brief.sources:
        cte_sql = _build_source_cte(src, brief.primary_dialect)
        if cte_sql: ctes.append(cte_sql); last_cte = src.cte_name
    for tx in brief.transforms:
        cte_sql = None
        if tx.role == "transformer" and tx.select_expressions:
            cte_sql = _build_tmap_cte(tx, brief.flow_to_cte)
        elif tx.role == "filter":
            cte_sql = _build_filter_cte(tx, brief.flow_to_cte)
        elif tx.role == "aggregate" and tx.aggregate_funcs:
            cte_sql = _build_aggregate_cte(tx)
        elif tx.role == "sort" and tx.sort_cols:
            cte_sql = _build_sort_cte(tx)
        elif tx.role == "dedup" and tx.dedup_keys:
            cte_sql = _build_dedup_cte(tx)
        elif tx.role == "union" and len(tx.input_ctes) > 1:
            cte_sql = _build_union_cte(tx)
        elif tx.role == "custom_code":
            cte_sql = _build_custom_code_cte(tx, brief.flow_to_cte, llm_fn)
        else:
            ic = tx.input_ctes[0] if tx.input_ctes else (last_cte or "dual")
            cte_sql = _build_passthrough_cte(tx.cte_name, ic)
        if cte_sql: ctes.append(cte_sql); last_cte = tx.cte_name
    if not ctes: return None
    final_cte = last_cte
    if brief.sinks:
        for sink in brief.sinks:
            if sink.input_cte: final_cte = sink.input_cte; break
    if not final_cte:
        final_cte = ctes[-1].split(" AS ")[0].strip() if ctes else "dual"
    header = [
        f"-- Migrated from Talend job: {brief.job_name}",
        f"-- Purpose: {brief.job_purpose}",
        f"-- Confidence: {brief.confidence:.0%}",
        f"-- Source dialect: {brief.primary_dialect}",
    ]
    if brief.has_custom_code:
        header.append("-- Contains custom Java code")
    if brief.has_api_sources:
        header.append("-- Contains API/SaaS sources -- requires Temporal")
    if brief.warnings:
        for w in brief.warnings[:3]: header.append(f"-- WARNING: {w}")
    mat = brief.materialization
    if mat == 'snapshot':
        header.append("")
        header.append("{{ config(materialized='table') }}  {# TODO: convert to dbt snapshot #}")
    else:
        header.append("")
        header.append("{{ config(materialized='" + mat + "') }}")
    model = "\n".join(header) + "\n\n"
    model += "WITH\n\n"
    model += ",\n\n".join(ctes)
    model += f"\n\nSELECT * FROM {final_cte}\n"
    if HAS_SQLGLOT:
        try:
            clean_lines = []
            for line in model.splitlines():
                s = line.strip()
                if s.startswith('{{') and ('}}' in s): continue
                if s.startswith('--'): continue
                clean_lines.append(line)
            clean = '\n'.join(clean_lines)
            clean = re.sub(r"'\{\{.*?\}\}'", "'placeholder'", clean)
            clean = re.sub(r'\{\{.*?\}\}', "'placeholder'", clean)
            clean = re.sub(r'\{#.*?#\}', '', clean)
            clean = re.sub(r'/\*.*?\*/', '', clean, flags=re.DOTALL)
            sqlglot.parse_one(clean, read="duckdb", error_level=sqlglot.ErrorLevel.WARN)
        except Exception as e:
            model += f"\n-- VALIDATION WARNING: {str(e)[:100]}\n"
    return model
