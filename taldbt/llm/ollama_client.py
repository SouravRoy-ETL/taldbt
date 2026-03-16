"""
Ollama LLM Client: direct HTTP calls to local Ollama server.
No langchain dependency. Uses qwen3-coder:30b for:
  - Java→SQL translation (expressions the knowledge_base can't handle)
  - Unknown component handling
  - Self-healing (fix DuckDB syntax errors)

Only called for the ~5-15% of expressions that fail deterministic translation.
"""
from __future__ import annotations
import json
import re
import requests
from typing import Optional
from taldbt.models.ast_models import ComponentAST, ComponentBehavior


# Legacy constants kept for backward compatibility
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "qwen3-coder:30b"


def _call_ollama(prompt: str, temperature: float = 0.1, max_tokens: int = 4096) -> str:
    """Send prompt to the active LLM provider (local or cloud).
    Routes through llm_provider for unified endpoint support."""
    from taldbt.llm.llm_provider import llm_complete
    return llm_complete(prompt=prompt, temperature=temperature, max_tokens=max_tokens)


def _extract_sql(raw: str) -> str:
    """Extract SQL from LLM response, stripping markdown fences and thinking blocks."""
    # Remove <think>...</think> blocks (qwen3 reasoning)
    raw = re.sub(r'<think>.*?</think>', '', raw, flags=re.DOTALL)
    # Remove markdown code fences
    raw = re.sub(r'```sql\s*', '', raw)
    raw = re.sub(r'```\s*', '', raw)
    return raw.strip()


# ═══════════════════════════════════════════════════════════
# Java Expression Translation
# ═══════════════════════════════════════════════════════════

JAVA_TO_SQL_PROMPT = """You are a senior data engineer converting Talend Java code to DuckDB SQL.

TASK: Convert this Java expression/code to a single DuckDB SQL expression.

INPUT COLUMNS AVAILABLE: {columns}
JAVA CODE:
{java_code}

RULES:
- Output ONLY the SQL expression, no explanation, no SELECT wrapper
- Use DuckDB syntax (not MySQL, not PostgreSQL, not Snowflake)
- Replace Java ternary (a ? b : c) → CASE WHEN a THEN b ELSE c END
- Replace .equals("x") → = 'x'
- Replace .toUpperCase() → UPPER()
- Replace .toLowerCase() → LOWER()
- Replace .trim() → TRIM()
- Replace .substring(a,b) → SUBSTRING(col, a+1, b-a)
- Replace null checks → IS NULL / IS NOT NULL / COALESCE
- Replace String concatenation (+) → ||
- Replace TalendDate.getCurrentDate() → CURRENT_TIMESTAMP
- Replace TalendDate.formatDate("fmt", date) → STRFTIME(date, 'fmt')
- Replace TalendDate.parseDate("fmt", str) → STRPTIME(str, 'fmt')
- Replace Numeric.sequence("name", start, step) → ROW_NUMBER() OVER ()
- Replace StringHandling.UPCASE(x) → UPPER(x)
- Replace Relational.ISNULL(x) → (x IS NULL)
- Replace context.xxx → {{ var('xxx') }}
- Replace Pid → {{ invocation_id }}
- Replace globalMap.get("key") → {{ var('key') }}  (best approximation)
- Replace == with = (for non-null comparisons)
- Replace && with AND, || with OR (boolean context)
- Java "strings" become SQL 'strings'

SQL:"""


def translate_java_expression(java_expr: str, available_columns: list[str] = None) -> str:
    """Translate a single Java expression to DuckDB SQL using Ollama."""
    cols = ", ".join(available_columns) if available_columns else "unknown"
    prompt = JAVA_TO_SQL_PROMPT.format(columns=cols, java_code=java_expr)
    raw = _call_ollama(prompt)
    return _extract_sql(raw)


# ═══════════════════════════════════════════════════════════
# Full Component Translation
# ═══════════════════════════════════════════════════════════

COMPONENT_TRANSLATE_PROMPT = """You are a senior data engineer migrating Talend ETL to dbt/DuckDB.

TASK: Generate a SQL CTE for this Talend component.

COMPONENT TYPE: {comp_type}
PARAMETERS:
{params}

INPUT CTEs AVAILABLE: {inputs}
OUTPUT SCHEMA: {schema}

RULES:
- Generate a CTE named "{cte_name}" in format: {cte_name} AS (SELECT ...)
- Use DuckDB SQL syntax only
- Reference input CTEs by name in FROM/JOIN clauses
- Output must match the output schema column names
- No markdown, no explanation, ONLY the SQL CTE
- For tJavaRow: translate output_row.x = input_row.y → y AS x in SELECT
- For tJavaRow: translate output_row.x = expression → translated_expression AS x
- Replace all Java constructs with SQL equivalents

SQL:"""


def translate_component(
    comp: ComponentAST,
    input_ctes: list[str],
    flow_map: dict[str, str],
) -> Optional[str]:
    """Translate an unknown/custom component to SQL using Ollama."""
    params_str = "\n".join(
        f"  {k}: {v[:200]}"
        for k, v in comp.parameters.items()
        if k not in ("UNIQUE_NAME", "CONNECTION_FORMAT") and v
    )

    schema_cols = []
    for connector, cols in comp.schemas.items():
        for c in cols:
            schema_cols.append(f"{c.name} ({c.sql_type})")

    inputs_str = ", ".join(input_ctes) if input_ctes else "none"
    schema_str = ", ".join(schema_cols) if schema_cols else "unknown"

    # For custom code, include the Java
    if comp.behavior == ComponentBehavior.CUSTOM_CODE and comp.java_code:
        params_str += f"\n  JAVA_CODE:\n{comp.java_code[:2000]}"

    prompt = COMPONENT_TRANSLATE_PROMPT.format(
        comp_type=comp.component_type,
        params=params_str,
        inputs=inputs_str,
        schema=schema_str,
        cte_name=comp.unique_name,
    )

    raw = _call_ollama(prompt)
    sql = _extract_sql(raw)

    if not sql or sql.startswith("-- ERROR"):
        return None

    # Ensure it's a proper CTE
    if not sql.upper().startswith(comp.unique_name.upper()):
        sql = f"{comp.unique_name} AS (\n    {sql}\n)"

    return sql


# ═══════════════════════════════════════════════════════════
# Self-Healing: fix DuckDB errors
# ═══════════════════════════════════════════════════════════

SELF_HEAL_PROMPT = """The following DuckDB SQL has a syntax error. Fix it.

SQL:
{sql}

ERROR MESSAGE:
{error}

AVAILABLE CTEs/TABLES: {tables}

Return ONLY the corrected SQL. No explanation. No markdown fences."""


def self_heal(failed_sql: str, error: str, available_tables: list[str], attempt: int = 1) -> Optional[str]:
    """Feed a DuckDB error back to Ollama and get a fix. Max 3 attempts."""
    if attempt > 3:
        return None

    tables_str = ", ".join(available_tables)
    prompt = SELF_HEAL_PROMPT.format(sql=failed_sql, error=error, tables=tables_str)
    raw = _call_ollama(prompt)
    sql = _extract_sql(raw)

    return sql if sql and not sql.startswith("-- ERROR") else None


# ═══════════════════════════════════════════════════════════
# UNIVERSAL MODEL GENERATION (the brain)
# Ollama reviews deterministic output and fixes/completes it.
# This is what handles ANY component combination ever created.
# ═══════════════════════════════════════════════════════════

UNIVERSAL_REVIEW_PROMPT = """You are a senior data engineer reviewing a machine-generated dbt model.
The model was auto-translated from a Talend ETL job. Your job is to FIX any issues.

JOB NAME: {job_name}
JOB PURPOSE: {job_purpose}
SOURCE DIALECT: {dialect}
COMPONENTS: {components}
FLOW MAP: {flow_map}

DETERMINISTIC SQL (machine-generated, may have issues):
{deterministic_sql}

KNOWN ISSUES:
{issues}

RULES:
- Fix any UNRESOLVED references — figure out what the original expression intended
- Fix any TODO [AI] markers — translate the Java to DuckDB SQL
- Fix any syntax errors for DuckDB
- Keep the CTE structure. Do NOT flatten into a single query.
- Keep {{ config() }}, {{ source() }}, {{ var() }} Jinja tags as-is
- Use DuckDB syntax ONLY (not MySQL, not Postgres, not Snowflake)
- If a column expression is truly unknowable, use NULL AS column_name /* unknown */
- Return ONLY the complete corrected SQL model. No explanation. No markdown fences.

CORRECTED SQL:"""


def review_and_fix_model(
    job_name: str,
    job_purpose: str,
    dialect: str,
    components: list[str],
    flow_map: dict[str, str],
    deterministic_sql: str,
) -> Optional[str]:
    """Send deterministic output to Ollama for review and correction.

    This is the universal handler. The deterministic engine generates a first pass.
    Ollama reviews it, fixes UNRESOLVED references, translates remaining Java,
    corrects syntax. If the deterministic output is clean (no issues), this
    returns None and the deterministic output is used as-is.
    """
    # Check if review is needed
    issues = []
    if 'UNRESOLVED' in deterministic_sql:
        count = deterministic_sql.count('UNRESOLVED')
        issues.append(f"{count} UNRESOLVED column references")
    if 'TODO [AI]' in deterministic_sql:
        count = deterministic_sql.count('TODO [AI]')
        issues.append(f"{count} TODO [AI] markers needing Java→SQL translation")
    if 'VALIDATION WARNING' in deterministic_sql:
        issues.append("sqlglot validation warning")
    if '_placeholder' in deterministic_sql:
        issues.append("Placeholder columns from unsupported sources")

    # No issues → deterministic output is good, skip LLM
    if not issues:
        return None

    comps_str = ", ".join(components[:20])
    flow_str = json.dumps(flow_map, indent=2) if flow_map else "{}"
    issues_str = "\n".join(f"- {i}" for i in issues)

    prompt = UNIVERSAL_REVIEW_PROMPT.format(
        job_name=job_name,
        job_purpose=job_purpose,
        dialect=dialect,
        components=comps_str,
        flow_map=flow_str,
        deterministic_sql=deterministic_sql,
        issues=issues_str,
    )

    raw = _call_ollama(prompt, temperature=0.1, max_tokens=8192)
    sql = _extract_sql(raw)

    if not sql or sql.startswith("-- ERROR"):
        return None  # Fall back to deterministic output

    return sql


UNIVERSAL_GENERATE_PROMPT = """You are a senior data engineer. Generate a complete dbt SQL model
from this Talend job specification. This job could NOT be handled by the deterministic engine.

JOB NAME: {job_name}
SOURCE DIALECT: {dialect}

COMPONENTS AND THEIR ROLES:
{component_briefs}

DATA FLOW (source → target):
{flow_description}

tMap DETAILS (if any):
{tmap_details}

RULES:
- Generate a complete dbt model with {{ config(materialized='table') }}
- Use WITH ... AS CTE structure
- Source tables: {{ source('schema', 'table') }}
- DuckDB SQL syntax ONLY
- Match the output schema from the sink component
- Handle JOINs correctly based on tMap join keys and innerJoin flag
- Translate ALL expressions from Java to DuckDB SQL
- Return ONLY SQL. No explanation. No markdown.

SQL:"""


def generate_model_from_scratch(
    job_name: str,
    dialect: str,
    component_briefs: str,
    flow_description: str,
    tmap_details: str = "",
) -> Optional[str]:
    """Generate a complete model from scratch when deterministic engine fails entirely."""
    prompt = UNIVERSAL_GENERATE_PROMPT.format(
        job_name=job_name,
        dialect=dialect,
        component_briefs=component_briefs,
        flow_description=flow_description,
        tmap_details=tmap_details or "No tMap in this job",
    )
    raw = _call_ollama(prompt, temperature=0.1, max_tokens=8192)
    sql = _extract_sql(raw)
    if not sql or sql.startswith("-- ERROR"):
        return None
    return sql


# ═══════════════════════════════════════════════════════════
# Status Check
# ═══════════════════════════════════════════════════════════

def check_ollama_status() -> dict:
    """Check LLM provider status (local Ollama or cloud).
    Returns dict compatible with UI expectations."""
    from taldbt.llm.llm_provider import check_provider_status
    return check_provider_status()
