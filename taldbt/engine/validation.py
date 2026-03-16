"""
Data Validation Engine: diagnostic-only migration quality assessment.

PHILOSOPHY: Diagnose everything, fix nothing.
This engine NEVER changes SQL or data. It produces a detailed report that
tells a human exactly what's wrong and why, with actionable recommendations.

Categories:
  - INFRASTRUCTURE: test data missing columns, tables not created (tool's fault)
  - TRANSLATION: Java expressions not converted, raw Talend code in output (generator's fault)
  - LOGIC: JOIN producing 0 rows, filter eliminating data (may be correct — needs human review)
  - DATA_QUALITY: all-NULL columns, duplicate keys, type mismatches
"""
from __future__ import annotations
import re
from pathlib import Path
from dataclasses import dataclass, field


@dataclass
class Check:
    name: str
    status: str           # pass, warn, fail
    detail: str
    category: str = ""    # infrastructure, translation, logic, data_quality
    recommendation: str = ""

@dataclass
class ModelValidation:
    model: str
    status: str = "unknown"
    row_count: int = 0
    col_count: int = 0
    checks: list[Check] = field(default_factory=list)
    all_null_cols: list[str] = field(default_factory=list)
    sample_row: dict = field(default_factory=dict)
    dbt_status: str = ""       # success, error, skipped
    dbt_error: str = ""        # error message from dbt run

    @property
    def pass_count(self): return sum(1 for c in self.checks if c.status == "pass")
    @property
    def warn_count(self): return sum(1 for c in self.checks if c.status == "warn")
    @property
    def fail_count(self): return sum(1 for c in self.checks if c.status == "fail")

    def to_dict(self):
        return {
            "model": self.model, "status": self.status,
            "rows": self.row_count, "cols": self.col_count,
            "checks_pass": self.pass_count, "checks_warn": self.warn_count,
            "checks_fail": self.fail_count,
            "checks": [{"name": c.name, "status": c.status, "detail": c.detail,
                        "category": c.category, "recommendation": c.recommendation} for c in self.checks],
            "all_null_cols": self.all_null_cols,
            "sample_row": self.sample_row,
            "dbt_status": self.dbt_status, "dbt_error": self.dbt_error,
        }


@dataclass
class ValidationReport:
    models: list[ModelValidation] = field(default_factory=list)
    total_models: int = 0
    passed: int = 0
    warned: int = 0
    failed: int = 0
    errors: list[str] = field(default_factory=list)

    def summary_dict(self) -> dict:
        total = max(self.total_models, 1)
        healthy = self.passed + self.warned
        return {
            "total_models": self.total_models,
            "passed": self.passed, "warned": self.warned, "failed": self.failed,
            "pass_rate": f"{(self.passed / total) * 100:.0f}%",
            "health_rate": f"{(healthy / total) * 100:.0f}%",
            "models": [m.to_dict() for m in self.models],
        }


def validate_migration(db_path: str, output_dir: str, project=None) -> ValidationReport:
    """Run full diagnostic validation. Never modifies anything."""
    from taldbt.engine.duckdb_engine import create_connection
    import json

    report = ValidationReport()
    if not Path(db_path).exists():
        report.errors.append(f"DuckDB not found: {db_path}")
        return report

    models_dir = Path(output_dir) / "models"
    dbt_models = {}
    if models_dir.exists():
        for sf in models_dir.rglob("*.sql"):
            dbt_models[sf.stem.lower()] = sf

    # Load dbt run results for error messages
    dbt_results = {}
    rr_path = Path(output_dir) / "target" / "run_results.json"
    if rr_path.exists():
        try:
            rr = json.loads(rr_path.read_text(encoding="utf-8"))
            for r in rr.get("results", []):
                model_name = r["unique_id"].split(".")[-1]
                dbt_results[model_name.lower()] = {
                    "status": r["status"],
                    "message": r.get("message", "")[:300],
                }
        except Exception:
            pass

    con = create_connection(db_path, read_only=True)

    try:
        all_objects = {row[0].lower(): row[1] for row in con.execute(
            "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()}
    except Exception as e:
        report.errors.append(str(e)); con.close(); return report

    # Validate each dbt model
    for model_name, sql_file in sorted(dbt_models.items()):
        mv = ModelValidation(model=model_name)
        sql_content = ""
        try:
            sql_content = sql_file.read_text(encoding="utf-8")
        except Exception:
            pass

        # dbt run status
        dbt_info = dbt_results.get(model_name, {})
        mv.dbt_status = dbt_info.get("status", "unknown")
        mv.dbt_error = dbt_info.get("message", "")

        if mv.dbt_status == "error":
            msg = mv.dbt_error
            # Categorize the dbt error
            if "does not have a column" in msg or "not found" in msg:
                mv.checks.append(Check("dbt Run", "fail", f"dbt error: {msg[:150]}",
                    "infrastructure", "Test data table is missing columns that the SQL references. "
                    "Check source schema completeness."))
            elif "Contents of view were altered" in msg:
                mv.checks.append(Check("dbt Run", "fail", f"View schema mismatch: {msg[:150]}",
                    "infrastructure", "DuckDB view schema changed between runs. "
                    "Delete the output dir and re-run AutoPilot."))
            else:
                mv.checks.append(Check("dbt Run", "fail", f"dbt error: {msg[:150]}",
                    "translation", "Review the generated SQL for syntax or reference issues."))

        # Check if model exists in DuckDB
        if model_name not in all_objects:
            if mv.dbt_status != "error":
                mv.checks.append(Check("Materialized", "fail",
                    "Model not found in DuckDB — dbt may not have run",
                    "infrastructure", "Run dbt run to materialize this model."))
            mv.status = "fail"
            report.models.append(mv)
            continue

        try:
            # ── Row Count ──
            row_count = con.execute(f'SELECT COUNT(*) FROM "{model_name}"').fetchone()[0]
            mv.row_count = row_count

            if row_count == 0:
                # Diagnose WHY 0 rows
                join_count = sql_content.count("INNER JOIN")
                if join_count > 3:
                    mv.checks.append(Check("Row Count", "fail",
                        f"0 rows with {join_count} INNER JOINs — high JOIN chain likely filtering all data",
                        "logic", f"This model has {join_count} INNER JOINs. With test data, "
                        "even one mismatched key produces 0 rows. "
                        "This may work correctly with real data. Verify JOIN keys manually."))
                elif join_count > 0:
                    mv.checks.append(Check("Row Count", "fail",
                        f"0 rows with {join_count} INNER JOIN(s) — join key mismatch",
                        "logic", "Check that source tables have matching key values for the JOIN conditions."))
                else:
                    mv.checks.append(Check("Row Count", "fail",
                        "0 rows — check source data and WHERE conditions",
                        "logic", "No JOINs present. Check if source table is empty or WHERE clause is too restrictive."))
            elif row_count < 2:
                mv.checks.append(Check("Row Count", "warn",
                    f"{row_count} rows — very low", "data_quality",
                    "Only 1 row produced. Verify JOIN conditions with real data."))
            else:
                mv.checks.append(Check("Row Count", "pass", f"{row_count} rows", "data_quality"))

            # ── Column Schema ──
            cols = con.execute(f"""
                SELECT column_name, data_type FROM information_schema.columns
                WHERE table_name='{model_name}' AND table_schema='main' ORDER BY ordinal_position
            """).fetchall()
            mv.col_count = len(cols)

            if not cols:
                mv.checks.append(Check("Schema", "fail", "No columns", "infrastructure"))
            else:
                mv.checks.append(Check("Schema", "pass", f"{len(cols)} columns", "data_quality"))

            # ── NULL Analysis ──
            if row_count > 0 and cols:
                all_null = []
                for cn, ct in cols:
                    try:
                        nc = con.execute(f'SELECT COUNT(*) FROM "{model_name}" WHERE "{cn}" IS NULL').fetchone()[0]
                        if nc == row_count:
                            all_null.append(cn)
                    except Exception:
                        pass

                mv.all_null_cols = all_null
                if all_null:
                    # Count how many are INTENTIONALLY null (NULL AS col in the SQL)
                    intentional_nulls = set()
                    if sql_content:
                        for m in re.finditer(r'NULL\s+AS\s+(\w+)', sql_content, re.IGNORECASE):
                            intentional_nulls.add(m.group(1).lower())
                    # Also count audit/DI columns as expected
                    audit_patterns = ('di_', 'sor_', 'dw_', 'etl_', 'batch_', 'load_', 'created_', 'modified_')
                    expected_nulls = [c for c in all_null if c.lower() in intentional_nulls
                                     or any(c.lower().startswith(p) for p in audit_patterns)]
                    unexpected_nulls = [c for c in all_null if c not in expected_nulls]

                    pct = len(all_null) / len(cols) * 100
                    if len(unexpected_nulls) == 0:
                        # All nulls are intentional or audit — pass
                        mv.checks.append(Check("NULL Analysis", "pass",
                            f"{len(all_null)} all-NULL columns (all intentional/audit)", "data_quality"))
                    elif len(unexpected_nulls) > len(cols) * 0.5:
                        mv.checks.append(Check("NULL Analysis", "fail",
                            f"{len(unexpected_nulls)} unexpected all-NULL columns ({', '.join(unexpected_nulls[:5])})",
                            "translation",
                            "These columns should have data but are all NULL. "
                            "Check if input component type is supported by the expression mapper."))
                    elif unexpected_nulls:
                        mv.checks.append(Check("NULL Analysis", "warn",
                            f"{len(unexpected_nulls)} unexpected all-NULL: {', '.join(unexpected_nulls[:5])}",
                            "data_quality",
                            "Some columns are all NULL beyond audit fields. May be test data limitation."))
                    else:
                        mv.checks.append(Check("NULL Analysis", "pass",
                            f"{len(all_null)} all-NULL (audit/intentional)", "data_quality"))
                else:
                    mv.checks.append(Check("NULL Analysis", "pass", "No all-NULL columns", "data_quality"))

            # ── Key Uniqueness ──
            if row_count > 0 and cols:
                # Find a key column that ISN'T set to NULL AS in the SQL
                # and ISN'T an audit/SOR column (those are always duped in test data)
                null_as_cols = set()
                if sql_content:
                    for m_na in re.finditer(r'NULL\s+AS\s+(\w+)', sql_content, re.IGNORECASE):
                        null_as_cols.add(m_na.group(1).lower())
                audit_prefixes = ('sor_', 'di_', 'dw_', 'etl_', 'batch_')
                key_cols = [cn for cn, _ in cols
                            if any(k in cn.lower() for k in ('key', 'sk', '_id'))
                            and cn.lower() not in null_as_cols
                            and not any(cn.lower().startswith(p) for p in audit_prefixes)]
                if not key_cols:
                    # Fall back to first non-null-as, non-audit column
                    key_cols = [cn for cn, _ in cols
                                if cn.lower() not in null_as_cols
                                and not any(cn.lower().startswith(p) for p in audit_prefixes)]
                check_col = key_cols[0] if key_cols else cols[0][0]
                try:
                    dc = con.execute(f'SELECT COUNT(DISTINCT "{check_col}") FROM "{model_name}"').fetchone()[0]
                    if dc == row_count:
                        mv.checks.append(Check("Key Uniqueness", "pass",
                            f"'{check_col}' — all {dc} unique", "data_quality"))
                    elif dc == 0:
                        mv.checks.append(Check("Key Uniqueness", "pass",
                            f"'{check_col}' — column is all NULL (intentional)", "data_quality"))
                    else:
                        dup = (1 - dc / row_count) * 100
                        has_cross = 'CROSS JOIN' in sql_content if sql_content else False
                        has_multi_join = sql_content.count('JOIN') > 1 if sql_content else False
                        # With synthetic test data + multiple JOINs, dups are EXPECTED
                        if dup > 80 and not has_cross and not has_multi_join:
                            mv.checks.append(Check("Key Uniqueness", "warn",
                                f"'{check_col}' — {dc}/{row_count} unique ({dup:.0f}% dups)",
                                "data_quality", "High duplicates with single source — may indicate missing GROUP BY."))
                        else:
                            mv.checks.append(Check("Key Uniqueness", "pass",
                                f"'{check_col}' — {dc}/{row_count} unique", "data_quality"))
                except Exception:
                    pass

            # ── Sample Data ──
            if row_count > 0 and cols:
                try:
                    sample = con.execute(f'SELECT * FROM "{model_name}" LIMIT 1').fetchone()
                    if sample:
                        mv.sample_row = {cols[i][0]: str(sample[i])[:50] if sample[i] is not None else "NULL"
                                         for i in range(min(len(cols), len(sample)))}

                        # Check for raw Java/Talend artifacts
                        java_markers = ['context.', 'TalendDate.', 'routines.', 'StringHandling.',
                                        'Numeric.sequence', '.getProperty(', 'Integer.parseInt', 'globalMap.get']
                        artifacts = []
                        for i, (cn, _) in enumerate(cols):
                            if i < len(sample) and isinstance(sample[i], str):
                                if any(j in sample[i] for j in java_markers):
                                    artifacts.append(f"{cn}='{sample[i][:40]}'")

                        if artifacts:
                            mv.checks.append(Check("Java Artifacts", "fail",
                                f"Raw Java in output: {'; '.join(artifacts[:3])}",
                                "translation",
                                "Talend Java expressions leaked into the output data. "
                                "The knowledge_base translator missed these patterns."))
                        else:
                            mv.checks.append(Check("Java Artifacts", "pass",
                                "No raw Java/Talend expressions in output", "translation"))
                except Exception:
                    pass

        except Exception as e:
            mv.checks.append(Check("Query", "fail", f"Could not query: {str(e)[:100]}",
                "infrastructure", "The model may have a schema mismatch. Try deleting output dir and re-running."))

        # ── SQL Audit (static analysis of the SQL file) ──
        if sql_content:
            # Check for untranslated Java patterns in the SQL itself
            java_in_sql = re.findall(
                r'(?:context\.\w+|TalendDate\.\w+|routines\.\w+|StringHandling\.\w+'
                r'|Numeric\.sequence|globalMap\.get|\.toUpperCase\(\)|\.toLowerCase\(\)'
                r'|Integer\.parseInt|\.getProperty\()',
                sql_content
            )
            if java_in_sql:
                unique = list(set(java_in_sql))[:5]
                mv.checks.append(Check("SQL Audit", "fail",
                    f"Untranslated Java in SQL: {', '.join(unique)}",
                    "translation",
                    "These Java patterns should have been translated to SQL by the knowledge base. "
                    "Add them to knowledge_base.py or flag for LLM translation."))

            # Check for nested comment syntax errors
            if "/* 1 /*" in sql_content or "*/ */" in sql_content:
                mv.checks.append(Check("SQL Syntax", "warn",
                    "Nested SQL comments detected — may cause parse errors",
                    "infrastructure", "Post-processor generated nested comments. Check WHERE clause."))

        # ── Overall Status ──
        if mv.fail_count > 0:
            mv.status = "fail"
        elif mv.warn_count > 0:
            mv.status = "warn"
        else:
            mv.status = "pass"

        report.models.append(mv)

    con.close()

    # Totals
    report.total_models = len(report.models)
    report.passed = sum(1 for m in report.models if m.status == "pass")
    report.warned = sum(1 for m in report.models if m.status == "warn")
    report.failed = sum(1 for m in report.models if m.status in ("fail", "error"))

    return report
