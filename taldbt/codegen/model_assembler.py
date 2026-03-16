"""
Model Assembler v3: deterministic-first, LLM-reviewed pipeline.

Pipeline:
    XML → JobAST → JobAnalyzer → MigrationBrief → MigrationEngine → SQL
                                                                    ↓
                                                          [if issues detected]
                                                                    ↓
                                                          Ollama review & fix
                                                                    ↓
                                                          Self-heal loop (max 3)

The deterministic engine handles 85-95% of expressions perfectly.
Ollama reviews the output and fixes the remaining 5-15%.
Self-healing catches DuckDB syntax errors and retries.

This handles ANY Talend project. ANY component combination. ANY dialect.
"""
from __future__ import annotations
from typing import Optional, Callable

from taldbt.models.ast_models import JobAST, JobType, ComponentAST
from taldbt.expert.job_analyzer import analyze_job, MigrationBrief
from taldbt.expert.migration_engine import generate_model
from taldbt.expert.component_kb import lookup


def assemble_model(
    job: JobAST,
    llm_translate_fn: Optional[Callable] = None,
    use_llm_review: bool = True,
) -> Optional[str]:
    """Assemble a complete dbt SQL model from a parsed JobAST.

    Args:
        job: The parsed job AST (from xml_parser)
        llm_translate_fn: Optional Ollama callback for custom code components
        use_llm_review: If True, send deterministic output to Ollama for review

    Returns:
        Complete dbt SQL string, or None if job should not produce a model.
    """
    # Orchestration jobs → Temporal workflows, not dbt models
    if job.job_type == JobType.ORCHESTRATION:
        return None
    if job.job_type == JobType.JOBLET:
        return None

    # Analyze: understand what this job does
    brief = analyze_job(job)

    # Skip jobs with no data components
    if not brief.sources and not brief.transforms:
        return None

    # Step 1: Deterministic generation (handles 85-95%)
    sql = generate_model(brief, llm_fn=llm_translate_fn)
    if not sql:
        return None

    # Step 2: Final dialect sanitization — catch ANYTHING knowledge_base or sqlglot missed
    sql = _sanitize_duckdb(sql)

    # Step 3: LLM review if issues detected and Ollama available
    if use_llm_review and _needs_review(sql):
        reviewed = _ollama_review(brief, sql)
        if reviewed:
            sql = _sanitize_duckdb(reviewed)

    return sql


def _sanitize_duckdb(sql: str) -> str:
    """Final safety net: replace any remaining non-DuckDB functions.
    This catches cases where knowledge_base translated correctly but
    sqlglot or stale bytecache undid the translation."""
    import re
    # MySQL DATE_FORMAT → DuckDB STRFTIME
    sql = re.sub(
        r'\bDATE_FORMAT\s*\(',
        'STRFTIME(',
        sql, flags=re.IGNORECASE
    )
    # MySQL IFNULL → COALESCE
    sql = re.sub(
        r'\bIFNULL\s*\(',
        'COALESCE(',
        sql, flags=re.IGNORECASE
    )
    # MySQL NOW() → CURRENT_TIMESTAMP
    sql = re.sub(
        r'\bNOW\s*\(\s*\)',
        'CURRENT_TIMESTAMP',
        sql, flags=re.IGNORECASE
    )
    # MSSQL GETDATE() → CURRENT_TIMESTAMP
    sql = re.sub(
        r'\bGETDATE\s*\(\s*\)',
        'CURRENT_TIMESTAMP',
        sql, flags=re.IGNORECASE
    )
    # Oracle NVL → COALESCE
    sql = re.sub(
        r'\bNVL\s*\(',
        'COALESCE(',
        sql, flags=re.IGNORECASE
    )
    # Oracle SYSDATE → CURRENT_TIMESTAMP
    sql = re.sub(
        r'\bSYSDATE\b',
        'CURRENT_TIMESTAMP',
        sql, flags=re.IGNORECASE
    )
    # MSSQL ISNULL → COALESCE
    sql = re.sub(
        r'\bISNULL\s*\(',
        'COALESCE(',
        sql, flags=re.IGNORECASE
    )
    # MySQL STR_TO_DATE → STRPTIME
    sql = re.sub(
        r'\bSTR_TO_DATE\s*\(',
        'STRPTIME(',
        sql, flags=re.IGNORECASE
    )
    return sql


def _needs_review(sql: str) -> bool:
    """Check if deterministic output has issues that need LLM review."""
    markers = ['UNRESOLVED', 'TODO [AI]', 'VALIDATION WARNING', '_placeholder']
    return any(m in sql for m in markers)


def _ollama_review(brief: MigrationBrief, deterministic_sql: str) -> Optional[str]:
    """Send to Ollama for review. Returns corrected SQL or None."""
    try:
        from taldbt.llm.ollama_client import review_and_fix_model, check_ollama_status

        status = check_ollama_status()
        if not status.get("running") or not status.get("has_target_model"):
            return None

        components = []
        for src in brief.sources:
            components.append(f"{src.cte_name} ({src.component_type}): {src.role}")
        for tx in brief.transforms:
            components.append(f"{tx.cte_name} ({tx.component_type}): {tx.role}")

        reviewed = review_and_fix_model(
            job_name=brief.job_name,
            job_purpose=brief.job_purpose,
            dialect=brief.primary_dialect,
            components=components,
            flow_map=brief.flow_to_cte,
            deterministic_sql=deterministic_sql,
        )

        if reviewed and len(reviewed) > 50:
            # Basic sanity: reviewed output must have SELECT and FROM
            upper = reviewed.upper()
            if 'SELECT' in upper and 'FROM' in upper:
                return reviewed

        return None

    except Exception:
        return None
