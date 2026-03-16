"""
Gen 2 SQL Generator: constructs SQL from AST using sqlglot.

PRINCIPLES:
  - NEVER copy raw Talend SQL verbatim. Parse → reconstruct.
  - ALL ref resolution happens here during generation. Zero postprocessors.
  - Flow graph (flow_name → CTE) drives column reference resolution.
  - sqlglot handles dialect translation (MySQL/MSSQL → DuckDB).
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

from taldbt.models.ast_models import (
    ComponentAST, ComponentBehavior, JobAST, TMapData, TMapInput,
    TMapExpression, FilterCondition, AggregateConfig, ColumnSchema,
    ExpressionStrategy, SourceType,
)
from taldbt.llm.knowledge_base import translate_expression


# ═══════════════════════════════════════════════════════════
# SQL Reconstruction (replaces raw copy + postprocessors)
# ═══════════════════════════════════════════════════════════

def _reconstruct_sql(raw: str) -> str:
    """Parse embedded Talend SQL and reconstruct for DuckDB.

    Handles: schema stripping, XML entity decoding, backtick removal,
    dialect translation. Uses sqlglot when available, regex fallback.
    """
    # Step 1: Clean XML entities
    sql = raw.strip().strip('"').strip("'")
    sql = sql.replace("&#10;", "\n").replace("&#13;", "").replace("&#9;", " ")
    sql = sql.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    sql = sql.replace("\\n", "\n").replace("\\t", " ")
    sql = sql.replace("`", "")

    # Step 2: Resolve context variables BEFORE SQL parsing
    sql = re.sub(r'"\s*\+\s*context\.(\w+)\s*\+\s*"', r"'{{ var('\1', '') }}'", sql)
    sql = re.sub(r"context\.getProperty\(['\"](\w+)['\"]\)", r"'{{ var('\1', '') }}'", sql)
    sql = re.sub(r"context\.(\w+)", r"'{{ var('\1', '') }}'", sql)

    # Step 3: Use sqlglot for proper SQL parsing + schema stripping
    if HAS_SQLGLOT:
        try:
            # Try parsing as MySQL first (most common Talend source)
            parsed = sqlglot.parse_one(sql, read="mysql", error_level=sqlglot.ErrorLevel.IGNORE)

            # Strip schema prefixes: Schema.TableName → TableName
            for table in parsed.find_all(sqlexp.Table):
                if table.db:
                    table.set("db", None)
                if table.catalog:
                    table.set("catalog", None)

            # Strip schema from column refs: Schema.Table.Column → Table.Column
            for col in parsed.find_all(sqlexp.Column):
                if col.table and col.args.get("catalog"):
                    col.set("catalog", None)

            result = parsed.sql(dialect="duckdb")
            # sqlglot strips FROM schema but not column prefixes — clean those too
            result = re.sub(r'\b(\w+)\.(\w+)\.(\w+)\b', r'\2.\3', result)
            return result
        except Exception:
            pass  # sqlglot failed, use regex fallback

    # Step 4: Regex fallback (same as before but cleaner)
    # 3-part refs: Schema.Table.Column → Table.Column
    sql = re.sub(r'\b(\w+)\.(\w+)\.(\w+)\b', r'\2.\3', sql)
    # 2-part after FROM/JOIN: FROM Schema.Table → FROM Table
    sql = re.sub(r'(FROM|JOIN)\s+(\w+)\.(\w+)', r'\1 \3', sql, flags=re.IGNORECASE)

    return sql.strip()


def _tmap_join_type_to_sql(join_type: str) -> str:
    jt = join_type.upper().replace("_JOIN", "").replace("_", " ").strip()
    mapping = {
        "INNER": "INNER JOIN", "LEFT_OUTER": "LEFT JOIN", "LEFT OUTER": "LEFT JOIN",
        "RIGHT_OUTER": "RIGHT JOIN", "RIGHT OUTER": "RIGHT JOIN",
        "FULL_OUTER": "FULL OUTER JOIN", "FULL OUTER": "FULL OUTER JOIN",
        "CROSS": "CROSS JOIN", "LEFT": "LEFT JOIN", "RIGHT": "RIGHT JOIN",
    }
    return mapping.get(jt, "INNER JOIN")


# ═══════════════════════════════════════════════════════════
# Expression Resolution (replaces postprocessor)
# ═══════════════════════════════════════════════════════════

def _translate_and_resolve(raw_expr: str, flow_to_cte: dict[str, str]) -> tuple[str, ExpressionStrategy]:
    """Translate Java→SQL, then resolve flow refs to CTE names.

    This is the ONLY path through which tMap expressions should flow.
    """
    if not raw_expr or not raw_expr.strip():
        return "NULL", ExpressionStrategy.DETERMINISTIC

    translated, strategy = translate_expression(raw_expr)
    resolved = _resolve_flow_to_cte(translated, flow_to_cte)
    return resolved, strategy


# Java reserved words that Talend escapes with leading underscore in tMap expressions.
# e.g., DB column "Class" becomes "_Class" in tMap because "class" is a Java keyword.
_JAVA_RESERVED = frozenset({
    'class', 'order', 'group', 'default', 'table', 'column', 'index',
    'key', 'type', 'value', 'name', 'select', 'where', 'from', 'join',
    'union', 'insert', 'update', 'delete', 'create', 'drop', 'alter',
    'null', 'true', 'false', 'int', 'long', 'float', 'double', 'boolean',
    'char', 'string', 'byte', 'short', 'void', 'return', 'new', 'this',
    'super', 'abstract', 'final', 'static', 'public', 'private', 'protected',
})


def _resolve_flow_to_cte(expr: str, flow_to_cte: dict[str, str]) -> str:
    """Replace flow name references with CTE names.

    Also handles:
    - Java reserved word escaping: _Class → Class (Talend prepends _ for Java keywords)
    - Case-insensitive flow matching (Talend is inconsistent with casing)
    """
    if not flow_to_cte:
        return expr

    # Build case-insensitive lookup
    ci_map = {k.lower(): v for k, v in flow_to_cte.items()}
    # Also include CTE names themselves as valid prefixes
    cte_names = set(flow_to_cte.values())

    def _replace(m):
        prefix = m.group(1)
        col = m.group(2)

        # Already a CTE reference (tDBInput_1.col) → keep as-is
        if prefix in cte_names:
            # But fix Java reserved word escaping: tDBInput_1._Class → tDBInput_1.Class
            if col.startswith('_') and col[1:].lower() in _JAVA_RESERVED:
                return f"{prefix}.{col[1:]}"
            return m.group(0)

        # Flow name → CTE name
        mapped = ci_map.get(prefix.lower())
        if mapped:
            # Fix Java reserved word escaping in column name
            if col.startswith('_') and col[1:].lower() in _JAVA_RESERVED:
                col = col[1:]
            return f"{mapped}.{col}"

        # Unresolvable: prefix is not a known flow or CTE.
        # This is a broken tMap reference (stale input, deleted flow, etc.)
        # Leave as-is — validation will catch and diagnose it.
        return m.group(0)

    return re.sub(r'\b([a-zA-Z]\w+)\.(\w+)\b', _replace, expr)


# ═══════════════════════════════════════════════════════════
# CTE Generators by Component Behavior
# ═══════════════════════════════════════════════════════════

def generate_input_cte(comp: ComponentAST) -> Optional[str]:
    """Generate CTE for a DATA_SOURCE component."""
    si = comp.source_info
    if not si:
        return None

    name = comp.unique_name

    # ── Database source: reconstruct SQL from parsed query ──
    if si.source_type == SourceType.DATABASE:
        if si.query and si.query.cleaned_sql:
            reconstructed = _reconstruct_sql(si.query.cleaned_sql)
            return f"{name} AS (\n    {reconstructed}\n)"
        elif si.connection and si.connection.table:
            table = si.connection.table.replace('"', '').replace("'", "")
            # Strip schema prefix
            table = table.split(".")[-1] if "." in table else table
            cols = _build_select_list(comp)
            return f"{name} AS (\n    SELECT {cols} FROM {table}\n)"

    # ── File source: read_csv / read_json / read_parquet ──
    if si.source_type in (SourceType.FILE_CSV, SourceType.FILE_EXCEL,
                          SourceType.FILE_JSON, SourceType.FILE_XML,
                          SourceType.FILE_PARQUET):
        return _generate_file_cte(comp, name)

    # ── API/SaaS source: placeholder ──
    if si.source_type in (SourceType.API_REST, SourceType.API_SOAP, SourceType.SAAS):
        url = si.url or "unknown"
        return (f"{name} AS (\n"
                f"    -- TODO: API source ({comp.component_type})\n"
                f"    -- URL: {url}\n"
                f"    -- Implement as Temporal activity → staging table\n"
                f"    SELECT NULL AS _placeholder\n"
                f")")

    # ── Fallback: table scan ──
    table = comp.parameters.get("TABLE", "").replace('"', '').replace("'", "")
    if table:
        table = table.split(".")[-1] if "." in table else table
        cols = _build_select_list(comp)
        return f"{name} AS (\n    SELECT {cols} FROM {table}\n)"

    return None


def _build_select_list(comp: ComponentAST) -> str:
    """Build SELECT column list from component schema."""
    if "FLOW" in comp.schemas:
        cols = comp.schemas["FLOW"]
    elif comp.schemas:
        cols = list(comp.schemas.values())[0]
    else:
        return "*"

    if not cols:
        return "*"

    return ", ".join(f'"{c.name}"' for c in cols)


def _generate_file_cte(comp: ComponentAST, name: str) -> str:
    """Generate CTE for file-based input (CSV, Excel, JSON, Parquet)."""
    si = comp.source_info
    path = si.file_path or ""
    src_type = si.source_type

    if src_type == SourceType.FILE_CSV:
        sep = si.delimiter or ","
        header = "true" if si.has_header else "false"
        encoding = si.encoding or "UTF-8"
        return (f"{name} AS (\n"
                f"  SELECT * FROM read_csv('{path}', \n"
                f"               sep='{sep}', header={header}, "
                f"quote='\"', escape='\"', encoding='{encoding}')\n)")

    elif src_type == SourceType.FILE_EXCEL:
        return (f"{name} AS (\n"
                f"  SELECT * FROM read_csv('{path}', header=true, auto_detect=true)\n)")

    elif src_type == SourceType.FILE_JSON:
        return (f"{name} AS (\n"
                f"  SELECT * FROM read_json_auto('{path}')\n)")

    elif src_type == SourceType.FILE_PARQUET:
        return (f"{name} AS (\n"
                f"  SELECT * FROM read_parquet('{path}')\n)")

    elif src_type == SourceType.FILE_XML:
        return (f"{name} AS (\n"
                f"  -- TODO: XML file input — convert to CSV/JSON first\n"
                f"  -- Path: {path}\n"
                f"  SELECT NULL AS _placeholder\n)")

    return f"{name} AS (\n  SELECT * FROM read_csv_auto('{path}')\n)"


# ═══════════════════════════════════════════════════════════
# tMap CTE Generator (the big one)
# ═══════════════════════════════════════════════════════════

def generate_tmap_cte(comp: ComponentAST, flow_to_cte: dict[str, str]) -> Optional[str]:
    """Generate CTE for a tMap component with full expression translation."""
    td = comp.tmap_data
    if not td or not td.inputs:
        return None

    # Find main input and lookups
    main_input = None
    lookups = []
    for inp in td.inputs:
        if inp.is_main_input:
            main_input = inp
        else:
            lookups.append(inp)
    if not main_input and td.inputs:
        main_input = td.inputs[0]
    if not main_input:
        return None

    # Find the non-reject output table
    output = None
    for out in td.outputs:
        if not out.is_reject:
            output = out
            break
    if not output and td.outputs:
        output = td.outputs[0]

    # ── Build Var table (intermediate computations) ──
    var_cte_lines = []
    if td.var_table:
        for var_entry in td.var_table:
            if var_entry.expression:
                translated, _ = _translate_and_resolve(var_entry.expression, flow_to_cte)
                var_cte_lines.append((var_entry.name, translated))

    # ── Build FROM clause ──
    main_cte = flow_to_cte.get(main_input.name, main_input.name)
    from_clause = f"    FROM {main_cte}"

    # ── Build JOIN clauses ──
    join_clauses = []
    for lookup in lookups:
        lookup_cte = flow_to_cte.get(lookup.name, lookup.name)
        sql_join = _tmap_join_type_to_sql(lookup.join_type)

        if lookup.join_keys:
            on_parts = []
            for jk in lookup.join_keys:
                # Resolve the join key expression through the flow map
                resolved_expr, _ = _translate_and_resolve(jk.expression, flow_to_cte)
                on_parts.append(f"{lookup_cte}.{jk.name} = {resolved_expr}")
            join_clauses.append(f"    {sql_join} {lookup_cte} ON {' AND '.join(on_parts)}")
        else:
            join_clauses.append(f"    CROSS JOIN {lookup_cte}")

    # ── Build SELECT columns ──
    select_cols = []
    llm_flags = []

    if output and output.columns:
        for col in output.columns:
            raw_expr = col.expression
            if raw_expr and raw_expr.strip():
                translated, strategy = _translate_and_resolve(raw_expr, flow_to_cte)

                # Resolve Var table references
                for var_name, var_sql in var_cte_lines:
                    translated = re.sub(
                        rf'\bVar\.{re.escape(var_name)}\b',
                        f"({var_sql})",
                        translated,
                    )

                # Check for unresolvable table refs in the expression
                known = set(flow_to_cte.values()) | {k.lower() for k in flow_to_cte}
                has_bad_ref = False
                for rm in re.finditer(r'\b([a-zA-Z]\w+)\.(\w+)\b', translated):
                    p = rm.group(1)
                    if p not in known and p.lower() not in known and not p.startswith('t'):
                        has_bad_ref = True
                        break

                if has_bad_ref:
                    select_cols.append(f"    NULL AS {col.column_name} /* UNRESOLVED: {raw_expr.strip()[:60]} */")
                elif strategy == ExpressionStrategy.LLM_REQUIRED:
                    llm_flags.append(col.column_name)
                    select_cols.append(f"    {translated} AS {col.column_name} /* TODO [AI]: needs review */")
                elif translated != col.column_name:
                    select_cols.append(f"    {translated} AS {col.column_name}")
                else:
                    select_cols.append(f"    {translated}")
            else:
                # No expression in tMap output column.
                # This means the Talend developer left the mapping blank.
                select_cols.append(f"    NULL AS {col.column_name}")
    else:
        select_cols.append("    *")

    # ── Build WHERE clause from output filter ──
    where_clause = ""
    if output and output.filter and output.filter.expression:
        translated_filter, _ = _translate_and_resolve(output.filter.expression, flow_to_cte)

        # Check for unresolvable table refs in the filter.
        # If a table.column ref remains that isn't a known CTE, the filter is broken.
        known_prefixes = set(flow_to_cte.values())  # CTE names
        has_unresolved = False
        for fm in re.finditer(r'\b([a-zA-Z]\w+)\.(\w+)\b', translated_filter):
            if fm.group(1) not in known_prefixes and fm.group(1).lower() not in {k.lower() for k in flow_to_cte}:
                has_unresolved = True
                break

        if has_unresolved:
            where_clause = f"\n    -- WHERE {translated_filter} -- UNRESOLVED: stale tMap reference"
        else:
            where_clause = f"\n    WHERE {translated_filter}"

    # ── Assemble the CTE ──
    select_str = ",\n".join(select_cols)
    joins_str = "\n".join(join_clauses)

    body = f"    SELECT\n{select_str}\n{from_clause}"
    if joins_str:
        body += f"\n{joins_str}"
    if where_clause:
        body += where_clause

    if llm_flags:
        comp.confidence = max(0.3, comp.confidence - 0.1 * len(llm_flags))
        for flag in llm_flags:
            if flag not in comp.flagged:
                comp.flagged.append(flag)

    return f"{comp.unique_name} AS (\n{body}\n)"


# ═══════════════════════════════════════════════════════════
# Other Component CTE Generators
# ═══════════════════════════════════════════════════════════

def generate_filter_cte(comp: ComponentAST, input_cte: str) -> Optional[str]:
    if not comp.filter_conditions:
        return f"{comp.unique_name} AS (\n    SELECT * FROM {input_cte}\n)"
    conditions = []
    for fc in comp.filter_conditions:
        if fc.expression:
            translated, _ = translate_expression(fc.expression)
            conditions.append(translated)
    where = " AND ".join(conditions) if conditions else "TRUE"
    return f"{comp.unique_name} AS (\n    SELECT * FROM {input_cte}\n    WHERE {where}\n)"


def generate_aggregate_cte(comp: ComponentAST, input_cte: str) -> Optional[str]:
    ac = comp.aggregate_config
    if not ac:
        return f"{comp.unique_name} AS (\n    SELECT * FROM {input_cte}\n)"
    group_cols = ", ".join(f'"{g}"' for g in ac.group_by) if ac.group_by else ""
    agg_parts = []
    for a in ac.aggregates:
        func = a.function.upper()
        col = a.column
        alias = a.alias or f"{func}_{col}"
        if func == "COUNT":
            agg_parts.append(f'COUNT("{col}") AS "{alias}"')
        elif func in ("SUM", "AVG", "MIN", "MAX"):
            agg_parts.append(f'{func}("{col}") AS "{alias}"')
        else:
            agg_parts.append(f'{func}("{col}") AS "{alias}"')
    select = ", ".join([group_cols] + agg_parts) if group_cols else ", ".join(agg_parts)
    group_by = f"\n    GROUP BY {group_cols}" if group_cols else ""
    return f"{comp.unique_name} AS (\n    SELECT {select}\n    FROM {input_cte}{group_by}\n)"


def generate_sort_cte(comp: ComponentAST, input_cte: str) -> Optional[str]:
    sc = comp.sort_config
    if not sc or not sc.sort_keys:
        return f"{comp.unique_name} AS (\n    SELECT * FROM {input_cte}\n)"
    order = ", ".join(f'"{s.column}" {s.order.upper()}' for s in sc.sort_keys)
    return f"{comp.unique_name} AS (\n    SELECT * FROM {input_cte}\n    ORDER BY {order}\n)"


def generate_dedup_cte(comp: ComponentAST, input_cte: str) -> Optional[str]:
    dc = comp.dedup_config
    if not dc or not dc.key_columns:
        return f"{comp.unique_name} AS (\n    SELECT * FROM {input_cte}\n)"
    keys = ", ".join(f'"{k}"' for k in dc.key_columns)
    return (f"{comp.unique_name} AS (\n"
            f"    SELECT * FROM (\n"
            f"        SELECT *, ROW_NUMBER() OVER (PARTITION BY {keys}) AS _rn\n"
            f"        FROM {input_cte}\n"
            f"    ) WHERE _rn = 1\n)")


def generate_union_cte(comp: ComponentAST, input_ctes: list[str]) -> Optional[str]:
    if not input_ctes:
        return None
    parts = "\n    UNION ALL\n    ".join(f"SELECT * FROM {c}" for c in input_ctes)
    return f"{comp.unique_name} AS (\n    {parts}\n)"


def generate_passthrough_cte(comp: ComponentAST, input_cte: str) -> Optional[str]:
    return f"{comp.unique_name} AS (\n    SELECT * FROM {input_cte}\n)"


def generate_javarow_cte(comp: ComponentAST, input_cte: str,
                          flow_to_cte: dict[str, str]) -> Optional[str]:
    code = comp.java_code or ""
    if not code.strip():
        return f"{comp.unique_name} AS (\n    SELECT * FROM {input_cte}\n)"
    translated, strategy = translate_expression(code)
    if strategy == ExpressionStrategy.LLM_REQUIRED:
        return (f"{comp.unique_name} AS (\n"
                f"    -- TODO [AI]: tJavaRow needs LLM translation\n"
                f"    -- Java: {code[:150].replace(chr(10), ' ')}\n"
                f"    SELECT * FROM {input_cte}\n)")
    return f"{comp.unique_name} AS (\n    SELECT {translated}\n    FROM {input_cte}\n)"
