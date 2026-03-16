"""
E2E Pipeline Test: parses every .item file, generates SQL, validates it compiles.
No UI. No streamlit. Just truth.

Run: python -m taldbt.tests.e2e_pipeline_test <input_dir>

For each job:
  1. Parse XML -> JobAST
  2. Analyze -> MigrationBrief  
  3. Generate -> dbt SQL
  4. Validate SQL compiles (sqlglot parse, no runtime)
  5. Check for broken patterns (source() inside column names, etc.)

Reports: PASS / FAIL / SKIP for every job with exact error.
"""
from __future__ import annotations
import sys
import os
import re
import traceback
from pathlib import Path

# Add project to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from taldbt.parsers.xml_parser import TalendXMLParser
from taldbt.parsers.project_scanner import scan_project
from taldbt.expert.job_analyzer import analyze_job
from taldbt.expert.migration_engine import generate_model

try:
    import sqlglot
    HAS_SQLGLOT = True
except ImportError:
    HAS_SQLGLOT = False


class SQLValidator:
    """Validates generated SQL without executing it."""

    BROKEN_PATTERNS = [
        # source() ref leaked into column name: {{ source('x','y') }}.{{ source(
        (re.compile(r"\{\{\s*source\([^)]+\)\s*\}\}\.\{\{"), "source() ref inside column name"),
        # Double source ref in single token
        (re.compile(r"\{\{\s*source\([^)]+\)\s*\}\}[A-Z]"), "source() ref merged with column name"),
        # Empty CTE
        (re.compile(r"AS\s*\(\s*\)"), "Empty CTE body"),
        # SELECT with no columns
        (re.compile(r"SELECT\s+FROM", re.IGNORECASE), "SELECT with no columns"),
        # Dangling comma before FROM
        (re.compile(r",\s*\n\s*FROM", re.IGNORECASE), "Dangling comma before FROM"),
    ]

    @staticmethod
    def validate(sql: str) -> tuple[bool, list[str]]:
        """Validate SQL. Returns (is_valid, list_of_issues)."""
        issues = []

        if not sql or not sql.strip():
            return False, ["Empty SQL"]

        # Check for broken patterns
        for pattern, desc in SQLValidator.BROKEN_PATTERNS:
            matches = pattern.findall(sql)
            if matches:
                issues.append(f"BROKEN: {desc} ({len(matches)} occurrences)")

        # Check for UNRESOLVED markers
        unresolved = sql.count("UNRESOLVED")
        if unresolved > 0:
            issues.append(f"WARNING: {unresolved} UNRESOLVED references")

        # Check for TODO [AI] markers
        todos = sql.count("TODO [AI]")
        if todos > 0:
            issues.append(f"WARNING: {todos} TODO [AI] markers (need LLM)")

        # Check for _placeholder columns
        placeholders = sql.count("_placeholder")
        if placeholders > 0:
            issues.append(f"WARNING: {placeholders} placeholder columns")

        # sqlglot parse check (strip Jinja first)
        if HAS_SQLGLOT:
            clean = re.sub(r'\{\{.*?\}\}', "'__jinja__'", sql)
            clean = re.sub(r'\{#.*?#\}', '', clean)
            clean = re.sub(r'/\*.*?\*/', '', clean, flags=re.DOTALL)
            try:
                sqlglot.parse_one(clean, read="duckdb", error_level=sqlglot.ErrorLevel.RAISE)
            except Exception as e:
                issues.append(f"SYNTAX: {str(e)[:200]}")

        # Check has proper structure
        upper = sql.upper()
        if "WITH" not in upper:
            issues.append("STRUCTURE: Missing WITH clause")
        if "SELECT" not in upper:
            issues.append("STRUCTURE: Missing SELECT")
        if "FROM" not in upper:
            issues.append("STRUCTURE: Missing FROM")

        # Check has dbt patterns
        if "{{ config(" not in sql:
            issues.append("DBT: Missing {{ config() }}")
        if "{{ source(" not in sql and "read_csv" not in sql:
            issues.append("DBT: Missing {{ source() }} or file reader")

        has_fatal = any(i.startswith("BROKEN:") or i.startswith("SYNTAX:") for i in issues)
        return not has_fatal, issues


def run_e2e_test(input_dir: str) -> dict:
    """Run E2E test on all jobs in a Talend project directory."""

    print(f"{'='*70}")
    print(f"E2E PIPELINE TEST")
    print(f"Input: {input_dir}")
    print(f"{'='*70}\n")

    # Step 1: Scan project
    print("Scanning project...")
    try:
        project = scan_project(input_dir)
    except Exception as e:
        print(f"FATAL: Could not scan project: {e}")
        return {"fatal": str(e)}

    total = len(project.jobs)
    print(f"Found {total} jobs\n")

    results = {"pass": [], "fail": [], "skip": [], "warn": []}

    for name, job in sorted(project.jobs.items()):
        # Skip orchestration
        if job.job_type.value in ("ORCHESTRATION", "JOBLET"):
            results["skip"].append((name, "Orchestration/Joblet -> Temporal"))
            print(f"  SKIP  {name} (orchestration)")
            continue

        try:
            # Step 2: Analyze
            brief = analyze_job(job)

            if not brief.sources and not brief.transforms:
                results["skip"].append((name, "No data components"))
                print(f"  SKIP  {name} (no data components)")
                continue

            # Step 3: Generate
            sql = generate_model(brief)

            if not sql:
                results["skip"].append((name, "No SQL generated"))
                print(f"  SKIP  {name} (no SQL)")
                continue

            # Step 4: Validate
            is_valid, issues = SQLValidator.validate(sql)

            if is_valid and not issues:
                results["pass"].append((name, []))
                print(f"  PASS  {name}")
            elif is_valid and issues:
                results["warn"].append((name, issues))
                print(f"  WARN  {name}: {issues[0]}")
            else:
                results["fail"].append((name, issues))
                print(f"  FAIL  {name}: {issues[0]}")

        except Exception as e:
            results["fail"].append((name, [f"EXCEPTION: {str(e)}"]))
            print(f"  FAIL  {name}: {e}")
            traceback.print_exc()

    # Summary
    print(f"\n{'='*70}")
    print(f"RESULTS: {len(results['pass'])} pass, {len(results['warn'])} warn, "
          f"{len(results['fail'])} fail, {len(results['skip'])} skip / {total} total")
    print(f"{'='*70}")

    if results["fail"]:
        print(f"\nFAILURES:")
        for name, issues in results["fail"]:
            print(f"  {name}:")
            for i in issues:
                print(f"    - {i}")

    if results["warn"]:
        print(f"\nWARNINGS:")
        for name, issues in results["warn"]:
            print(f"  {name}:")
            for i in issues:
                print(f"    - {i}")

    return results


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m taldbt.tests.e2e_pipeline_test <talend_project_dir>")
        sys.exit(1)
    run_e2e_test(sys.argv[1])
