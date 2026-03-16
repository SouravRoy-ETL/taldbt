"""
AutoPilot Runner: orchestrates the full E2E migration pipeline.

1. Generate dbt models (same as normal migration)
2. Generate synthetic test data from source schemas
3. Load test data into DuckDB
4. Run dbt compile + run against DuckDB
5. Generate Temporal workflow files from the job DAG
6. Report results

This is the backend for the AutoPilot button in the UI.
"""
from __future__ import annotations
import os
import subprocess
import json
from pathlib import Path
from datetime import datetime
from typing import Callable, Optional

from taldbt.models.ast_models import ProjectAST, JobType
from taldbt.codegen.model_assembler import assemble_model
from taldbt.codegen.dbt_scaffolder import scaffold_dbt_project, write_model_file
from taldbt.engine.test_data_generator import (
    load_test_data_into_duckdb, write_test_data_sql,
    generate_file_sources, rewrite_file_paths_in_models,
)
from taldbt.orchestration.workflow_generator import generate_workflows
from taldbt.engine.validation import validate_migration


def _model_name(job_name: str) -> str:
    import re
    name = job_name.replace(" ", "_").lower()
    return re.sub(r'_\d+\.\d+$', '', name)


def run_autopilot(
    project: ProjectAST,
    output_dir: str,
    row_count: int = 20,
    skip_dead: bool = False,
    use_llm: bool = False,
    llm_translate_fn=None,
    log_fn: Optional[Callable[[str], None]] = None,
    progress_fn: Optional[Callable[[float, str], None]] = None,
) -> dict:
    """Run the full AutoPilot migration pipeline.

    Args:
        project: parsed ProjectAST
        output_dir: where to write the dbt project
        row_count: rows per test table
        skip_dead: whether to skip dead jobs
        use_llm: whether to use LLM for Java translation
        llm_translate_fn: LLM callback
        log_fn: callback for progress logging, signature: fn(message: str)

    Returns dict with:
        - models_generated: int
        - test_tables: int
        - test_rows: int
        - dbt_compile_ok: bool
        - dbt_run_results: list[dict]
        - temporal_files: list[str]
        - errors: list[str]
        - db_path: str
    """
    def log(msg: str):
        if log_fn:
            log_fn(msg)

    def prog(pct: float, step_text: str):
        if progress_fn:
            progress_fn(pct, step_text)

    results = {
        "models_generated": 0,
        "test_tables": 0,
        "test_rows": 0,
        "dbt_compile_ok": False,
        "dbt_run_results": [],
        "validation": None,
        "temporal_files": [],
        "errors": [],
        "db_path": "",
    }

    # STEP 0: Clean previous run artifacts
    prog(0.02, "Cleaning previous artifacts...")
    log("Cleaning previous run...")
    import shutil
    for db in Path(output_dir).glob("*.duckdb*"):
        try: db.unlink()
        except: pass
    models_path = Path(output_dir) / "models"
    if models_path.exists():
        for sql_file in models_path.rglob("*.sql"):
            try: sql_file.unlink()
            except: pass
    target_path = Path(output_dir) / "target"
    if target_path.exists():
        try: shutil.rmtree(str(target_path))
        except: pass
    td_path = Path(output_dir) / "test_data"
    if td_path.exists():
        try: shutil.rmtree(str(td_path))
        except: pass
    log("  Cleaned stale artifacts")

    # ═══════════════════════════════════════════════════
    # STEP 1: Scaffold dbt project
    # ═══════════════════════════════════════════════════
    prog(0.08, "Scaffolding dbt project...")
    log("📁 **Step 1/6:** Scaffolding dbt project...")
    try:
        scaffold_dbt_project(project, output_dir)
        log("  ✅ dbt_project.yml, profiles.yml, sources.yml, schema.yml")
    except Exception as e:
        results["errors"].append(f"Scaffold failed: {e}")
        log(f"  ❌ Scaffold failed: {e}")
        return results

    # ═══════════════════════════════════════════════════
    # STEP 2: Generate dbt SQL models
    # ═══════════════════════════════════════════════════
    prog(0.15, "Generating dbt SQL models...")
    log("🔄 **Step 2/6:** Generating dbt SQL models...")

    data_jobs = {n: j for n, j in project.jobs.items()
                 if j.job_type.value not in ("ORCHESTRATION", "JOBLET")}

    if skip_dead and project.dead_jobs:
        dead_set = set(project.dead_jobs)
        data_jobs = {n: j for n, j in data_jobs.items() if n not in dead_set}

    generated = 0
    for name, job in data_jobs.items():
        try:
            lower = name.lower()
            if any(kw in lower for kw in ("dim", "fact", "load_", "bridge_")):
                subfolder = "staging"
            elif any(lower.startswith(p) for p in ("stg_", "staging_", "src_")):
                subfolder = "staging"
            elif any(lower.startswith(p) for p in ("int_", "tmp_")):
                subfolder = "intermediate"
            else:
                subfolder = "marts"

            sql = assemble_model(job, llm_translate_fn=llm_translate_fn)
            if sql:
                write_model_file(sql, name, output_dir, subfolder)
                generated += 1
        except Exception as e:
            results["errors"].append(f"Model {name}: {e}")
            log(f"  ⚠️ {name}: {e}")

    results["models_generated"] = generated
    log(f"  ✅ {generated} dbt models generated")

    # ═══════════════════════════════════════════════════
    # STEP 3: Generate test data + load into DuckDB
    # ═══════════════════════════════════════════════════
    db_path = str(Path(output_dir) / "dev.duckdb")
    results["db_path"] = db_path

    # Remove old database if it exists
    if os.path.exists(db_path):
        os.remove(db_path)

    if row_count > 0:
        prog(0.35, "Generating test data with Faker...")
        log(f"🧪 **Step 3/6:** Generating test data ({row_count} rows/table)...")
        try:
            # 3a: Generate CSV/Excel files for file-based sources
            file_map = generate_file_sources(project, output_dir, row_count)
            if file_map:
                log(f"  📄 {len(file_map)} test data files generated")
                for orig, local in file_map.items():
                    if "ERROR" not in local:
                        log(f"    → `{Path(local).name}`")

                # 3b: Rewrite file paths in generated SQL models
                rewritten = rewrite_file_paths_in_models(output_dir, file_map)
                if rewritten:
                    log(f"  ✅ {rewritten} SQL models updated with local file paths")

            # 3c: Generate DuckDB tables
            sql_path = write_test_data_sql(project, output_dir, row_count)
            log(f"  📄 Test data SQL: `{sql_path}`")

            load_result = load_test_data_into_duckdb(project, db_path, row_count)
            results["test_tables"] = load_result["tables_created"]
            results["test_rows"] = load_result["total_rows"]

            if load_result["errors"]:
                for err in load_result["errors"][:5]:
                    results["errors"].append(f"Test data: {err}")
                    log(f"  ⚠️ {err}")

            flock_tag = "⚡ flock" if load_result.get("flock_available") else "standard"
            faker_tag = "🎭 Faker" if load_result.get("faker_available") else "basic"
            log(f"  ✅ {load_result['tables_created']} tables, "
                f"{load_result['total_rows']} rows loaded into `{db_path}` ({flock_tag} + {faker_tag})")
        except Exception as e:
            results["errors"].append(f"Test data generation failed: {e}")
            log(f"  ❌ Test data failed: {e}")
    else:
        log("⏭️ **Step 3/6:** Test data generation skipped (disabled)")

    # ═══════════════════════════════════════════════════
    # STEP 4: dbt compile + run
    # ═══════════════════════════════════════════════════
    prog(0.55, "Running dbt compile + run...")
    log("🏗️ **Step 4/6:** Running dbt compile + run...")

    dbt_ok = _run_dbt(output_dir, results, log)
    results["dbt_compile_ok"] = dbt_ok

    # ═══════════════════════════════════════════════════
    # STEP 5: Data Validation
    # ═══════════════════════════════════════════════════
    prog(0.75, "Validating migration output...")
    log("✅ **Step 5/6:** Validating migration output...")
    try:
        vr = validate_migration(db_path, output_dir, project)
        results["validation"] = vr.summary_dict()
        log(f"  📊 Validated {vr.total_models} models: "
            f"✅ {vr.passed} pass, ⚠️ {vr.warned} warn, ❌ {vr.failed} fail")
        for mv in vr.models:
            icon = "✅" if mv.status == "pass" else "⚠️" if mv.status == "warn" else "❌"
            fails = [c for c in mv.checks if c.status == "fail"]
            if fails:
                log(f"  {icon} {mv.model}: {fails[0].detail[:80]}")
    except Exception as e:
        log(f"  ⚠️ Validation error: {e}")

    # ═══════════════════════════════════════════════════
    # STEP 6: Generate Temporal workflows
    # ═══════════════════════════════════════════════════
    prog(0.90, "Generating Temporal workflows...")
    log("⚡ **Step 6/6:** Generating Temporal workflow files...")

    try:
        wf_result = generate_workflows(project, output_dir)
        results["temporal_files"] = wf_result["files_created"]

        if wf_result["errors"]:
            for err in wf_result["errors"]:
                results["errors"].append(f"Temporal: {err}")

        log(f"  ✅ {len(wf_result['files_created'])} Temporal files generated:")
        for f in wf_result["files_created"]:
            log(f"  📄 `{Path(f).name}`")
    except Exception as e:
        results["errors"].append(f"Temporal generation failed: {e}")
        log(f"  ❌ Temporal generation failed: {e}")

    # ═══════════════════════════════════════════════════
    # Summary
    # ═══════════════════════════════════════════════════
    log("")
    log("---")
    log(f"**🤖 AutoPilot Complete:**")
    log(f"  Models: {results['models_generated']} | "
        f"Test tables: {results['test_tables']} | "
        f"Test rows: {results['test_rows']}")
    log(f"  dbt run: {'✅ PASS' if dbt_ok else '⚠️ ISSUES (see above)'}")
    if results.get("validation"):
        v = results["validation"]
        log(f"  Validation: ✅ {v['passed']}/{v['total_models']} pass ({v['pass_rate']})")
    log(f"  Temporal files: {len(results['temporal_files'])}")
    prog(1.0, "Complete!")
    log(f"  **Validation report available in results below.**")
    if results["errors"]:
        log(f"  Errors: {len(results['errors'])}")

    return results


def _run_dbt(output_dir: str, results: dict, log: Callable) -> bool:
    """Run dbt compile and dbt run. Returns True if successful."""

    # Check if dbt is installed
    try:
        ver = subprocess.run(
            ["dbt", "--version"],
            capture_output=True, text=True, timeout=30,
        )
        if ver.returncode != 0:
            log("  ⚠️ dbt not found in PATH. Install with: `pip install dbt-duckdb`")
            log("  ⏭️ Skipping dbt compile/run")
            results["errors"].append("dbt not installed")
            return False
    except FileNotFoundError:
        log("  ⚠️ dbt not found in PATH. Install with: `pip install dbt-duckdb`")
        log("  ⏭️ Skipping dbt compile/run")
        results["errors"].append("dbt not installed")
        return False
    except Exception as e:
        log(f"  ⚠️ dbt check failed: {e}")
        results["errors"].append(f"dbt check: {e}")
        return False

    # Clean stale artifacts (views with mismatched schemas from previous runs)
    subprocess.run(
        ["dbt", "clean", "--project-dir", output_dir, "--profiles-dir", output_dir],
        capture_output=True, text=True, timeout=30,
    )

    # dbt compile
    log("  Running `dbt compile`...")
    try:
        compile_result = subprocess.run(
            ["dbt", "compile",
             "--project-dir", output_dir,
             "--profiles-dir", output_dir],
            capture_output=True, text=True, timeout=120,
        )
        if compile_result.returncode == 0:
            log("  ✅ dbt compile: PASS")
        else:
            err = compile_result.stderr[-300:] if compile_result.stderr else compile_result.stdout[-300:]
            log(f"  ⚠️ dbt compile issues: {err[:150]}")
            results["errors"].append(f"dbt compile: {err[:200]}")
    except Exception as e:
        log(f"  ⚠️ dbt compile failed: {e}")
        results["errors"].append(f"dbt compile: {e}")

    # dbt run
    log("  Running `dbt run`...")
    all_pass = True
    try:
        run_result = subprocess.run(
            ["dbt", "run",
             "--project-dir", output_dir,
             "--profiles-dir", output_dir],
            capture_output=True, text=True, timeout=300,
        )

        # Parse run_results.json if it exists
        rr_path = Path(output_dir) / "target" / "run_results.json"
        if rr_path.exists():
            try:
                rr = json.loads(rr_path.read_text(encoding="utf-8"))
                for node_result in rr.get("results", []):
                    model_id = node_result.get("unique_id", "unknown")
                    status = node_result.get("status", "unknown")
                    timing = node_result.get("execution_time", 0)
                    msg = node_result.get("message", "")

                    model_short = model_id.split(".")[-1] if "." in model_id else model_id
                    entry = {
                        "model": model_short,
                        "status": status,
                        "time": f"{timing:.1f}s",
                        "message": msg[:100] if msg else "",
                    }
                    results["dbt_run_results"].append(entry)

                    icon = "✅" if status == "success" else "❌"
                    log(f"  {icon} {model_short}: {status} ({timing:.1f}s)")
                    if status != "success":
                        all_pass = False
            except Exception:
                pass
        else:
            # No run_results.json — check return code
            if run_result.returncode != 0:
                all_pass = False
                err = run_result.stderr[-300:] if run_result.stderr else run_result.stdout[-300:]
                log(f"  ⚠️ dbt run failed: {err[:150]}")
                results["errors"].append(f"dbt run: {err[:200]}")
            else:
                log("  ✅ dbt run: PASS (no detailed results)")

    except subprocess.TimeoutExpired:
        log("  ⚠️ dbt run timed out after 5 minutes")
        results["errors"].append("dbt run timed out")
        all_pass = False
    except Exception as e:
        log(f"  ⚠️ dbt run failed: {e}")
        results["errors"].append(f"dbt run: {e}")
        all_pass = False

    return all_pass
