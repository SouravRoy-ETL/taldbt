"""
Temporal Workflow Generator: converts the Talend job DAG into executable
Temporal.IO Python workflow files.

Mapping:
  - Pure tParallelize jobs → @workflow.defn with asyncio.gather()
  - Mixed sequential + parallel jobs → @workflow.defn with await chains + gather
  - Each tRunJob → workflow.execute_child_workflow() or workflow.execute_activity()
  - Each leaf data job → activity that runs `dbt run --select model_name`

Output: ready-to-run Python files in the orchestration/ folder of the dbt project.
"""
from __future__ import annotations
import os
import re
from pathlib import Path
from textwrap import dedent, indent
from taldbt.models.ast_models import (
    ProjectAST, JobAST, JobType, TriggerType,
)


def _safe_class_name(job_name: str) -> str:
    """Convert job name to a valid Python class name."""
    name = re.sub(r'[^a-zA-Z0-9]', '_', job_name)
    name = re.sub(r'_+', '_', name).strip('_')
    # PascalCase
    return ''.join(word.capitalize() for word in name.split('_'))


def _safe_func_name(job_name: str) -> str:
    """Convert job name to a valid Python function name."""
    name = re.sub(r'[^a-zA-Z0-9]', '_', job_name).lower()
    return re.sub(r'_+', '_', name).strip('_')


def _model_name(job_name: str) -> str:
    """Convert Talend job name to dbt model name."""
    name = job_name.replace(" ", "_").lower()
    return re.sub(r'_\d+\.\d+$', '', name)


def _analyze_orchestration_job(job: JobAST, project: ProjectAST) -> dict:
    """Analyze an orchestration job to determine execution structure.

    Returns:
      {
        "steps": [
          {"type": "sequential", "job": "child_job_A"},
          {"type": "parallel", "jobs": ["child_job_B", "child_job_C"]},
          {"type": "sequential", "job": "child_job_D"},
        ]
      }
    """
    # Build internal graph: component → child job mapping
    comp_to_child = {}
    for comp in job.components.values():
        if comp.child_job_name:
            # Resolve to actual project job name
            child = comp.child_job_name
            for jname in project.jobs:
                if jname.lower().replace("_", "") == child.lower().replace("_", ""):
                    child = jname
                    break
            comp_to_child[comp.unique_name] = child

    # Find tParallelize components and their targets
    parallelize_targets = {}  # tParallelize_N → [child_job_names]
    for conn in job.connections:
        if conn.trigger_type == TriggerType.PARALLELIZE:
            src = conn.source
            tgt = conn.target
            if tgt in comp_to_child:
                parallelize_targets.setdefault(src, []).append(comp_to_child[tgt])

    # Find SUBJOB_OK sequential chains
    subjob_ok_chains = []  # [(src_comp, tgt_comp)]
    for conn in job.connections:
        if conn.trigger_type == TriggerType.SUBJOB_OK:
            subjob_ok_chains.append((conn.source, conn.target))

    # Build execution steps
    steps = []
    visited = set()

    # Simple case: pure tParallelize (all children run in parallel)
    if not subjob_ok_chains and parallelize_targets:
        for par_name, children in parallelize_targets.items():
            if len(children) > 1:
                steps.append({"type": "parallel", "jobs": children})
            elif children:
                steps.append({"type": "sequential", "job": children[0]})
        return {"steps": steps}

    # Complex case: mixed sequential + parallel chains
    # Walk the SUBJOB_OK chain
    # Find the root component (no incoming SUBJOB_OK)
    all_targets = {tgt for _, tgt in subjob_ok_chains}
    all_sources = {src for src, _ in subjob_ok_chains}
    root_comps = all_sources - all_targets

    # If root_comps include tRunJob directly, start there
    for root in sorted(root_comps):
        current = root
        while current:
            if current in comp_to_child:
                steps.append({"type": "sequential", "job": comp_to_child[current]})
                visited.add(current)
            elif current in parallelize_targets:
                children = parallelize_targets[current]
                if len(children) > 1:
                    steps.append({"type": "parallel", "jobs": children})
                elif children:
                    steps.append({"type": "sequential", "job": children[0]})
                visited.add(current)

            # Find next in chain
            next_comp = None
            for src, tgt in subjob_ok_chains:
                if src == current:
                    next_comp = tgt
                    break
            current = next_comp

    # Add any tRunJob children not in the chain (from parallelize)
    for comp_name, child in comp_to_child.items():
        if comp_name not in visited and child not in [
            s.get("job") for s in steps
        ] and child not in [
            j for s in steps if s["type"] == "parallel" for j in s["jobs"]
        ]:
            steps.append({"type": "sequential", "job": child})

    return {"steps": steps}


def _generate_workflow_class(job_name: str, structure: dict, project: ProjectAST) -> str:
    """Generate a @workflow.defn class for an orchestration job."""
    class_name = _safe_class_name(job_name) + "Workflow"
    steps = structure["steps"]

    body_lines = []
    for i, step in enumerate(steps):
        if step["type"] == "sequential":
            child = step["job"]
            child_job = project.jobs.get(child)
            if child_job and child_job.job_type == JobType.ORCHESTRATION:
                # Child is another orchestration job → execute_child_workflow
                child_class = _safe_class_name(child) + "Workflow"
                body_lines.append(
                    f'        await workflow.execute_child_workflow({child_class}.run, '
                    f'id=f"{{workflow.info().workflow_id}}-{_safe_func_name(child)}")'
                )
            else:
                # Leaf data job → execute_activity (dbt run)
                body_lines.append(
                    f'        await workflow.execute_activity('
                    f'run_dbt_model, args=["{_model_name(child)}"], '
                    f'start_to_close_timeout=timedelta(minutes=30))'
                )
            body_lines.append(f'        workflow.logger.info("Completed: {child}")')
            body_lines.append("")

        elif step["type"] == "parallel":
            children = step["jobs"]
            gather_parts = []
            for child in children:
                child_job = project.jobs.get(child)
                if child_job and child_job.job_type == JobType.ORCHESTRATION:
                    child_class = _safe_class_name(child) + "Workflow"
                    gather_parts.append(
                        f'            workflow.execute_child_workflow({child_class}.run, '
                        f'id=f"{{workflow.info().workflow_id}}-{_safe_func_name(child)}")'
                    )
                else:
                    gather_parts.append(
                        f'            workflow.execute_activity('
                        f'run_dbt_model, args=["{_model_name(child)}"], '
                        f'start_to_close_timeout=timedelta(minutes=30))'
                    )
            body_lines.append("        # Parallel execution (tParallelize WAIT=All)")
            body_lines.append("        await asyncio.gather(")
            body_lines.append(",\n".join(gather_parts))
            body_lines.append("        )")
            body_lines.append(f'        workflow.logger.info("Parallel group completed: {", ".join(children)}")')
            body_lines.append("")

    body = "\n".join(body_lines) if body_lines else "        pass  # no steps"

    return f'''
@workflow.defn
class {class_name}:
    """Temporal workflow for Talend job: {job_name}"""

    @workflow.run
    async def run(self) -> str:
        workflow.logger.info("Starting: {job_name}")

{body}
        workflow.logger.info("Completed: {job_name}")
        return "{job_name} completed"
'''


def generate_workflows(project: ProjectAST, output_dir: str) -> dict:
    """Generate all Temporal workflow files for the project.

    Creates:
      - workflows.py: all @workflow.defn classes
      - activities.py: dbt run activity
      - worker.py: worker registration + startup
      - run_workflow.py: CLI to trigger the root workflow

    Returns: {files_created: [str], errors: [str]}
    """
    orch_dir = Path(output_dir) / "orchestration"
    orch_dir.mkdir(parents=True, exist_ok=True)

    files_created = []
    errors = []

    # ── Identify orchestration jobs ───────────────────
    orch_jobs = {name: job for name, job in project.jobs.items()
                 if job.job_type == JobType.ORCHESTRATION}

    if not orch_jobs:
        errors.append("No orchestration jobs found — skipping Temporal generation")
        return {"files_created": files_created, "errors": errors}

    # ── Analyze each orchestration job ────────────────
    workflow_classes = []
    imported_classes = []

    for name, job in sorted(orch_jobs.items()):
        try:
            structure = _analyze_orchestration_job(job, project)
            code = _generate_workflow_class(name, structure, project)
            workflow_classes.append(code)
            imported_classes.append(_safe_class_name(name) + "Workflow")
        except Exception as e:
            errors.append(f"{name}: {str(e)[:100]}")

    # ── Write workflows.py ────────────────────────────
    workflows_code = f'''"""
Temporal Workflows — Auto-generated by taldbt from Talend orchestration DAG.

Orchestration jobs: {", ".join(orch_jobs.keys())}
Root workflow: {project.roots[0] if project.roots else "unknown"}

DO NOT EDIT — regenerate with taldbt AutoPilot.
"""
import asyncio
from datetime import timedelta
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import run_dbt_model

{"".join(workflow_classes)}
'''
    wf_path = orch_dir / "workflows.py"
    wf_path.write_text(workflows_code, encoding="utf-8")
    files_created.append(str(wf_path))

    # ── Write activities.py ───────────────────────────
    # Collect all leaf model names
    all_models = []
    for name, job in project.jobs.items():
        if job.job_type != JobType.ORCHESTRATION:
            all_models.append(_model_name(name))

    safe_output_dir = output_dir.replace("\\", "/")

    activities_code = f'''"""
Temporal Activities — dbt model execution.

Each activity runs a single dbt model via CLI.
Auto-generated by taldbt. Modify dbt_project_dir as needed.
"""
import subprocess
import os
from temporalio import activity

DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "{safe_output_dir}")


@activity.defn
async def run_dbt_model(model_name: str) -> str:
    """Execute a single dbt model: dbt run --select <model_name>"""
    activity.logger.info(f"Running dbt model: {{model_name}}")

    result = subprocess.run(
        ["dbt", "run", "--select", model_name,
         "--project-dir", DBT_PROJECT_DIR,
         "--profiles-dir", DBT_PROJECT_DIR],
        capture_output=True,
        text=True,
        timeout=600,
    )

    if result.returncode != 0:
        error_msg = result.stderr[-500:] if result.stderr else result.stdout[-500:]
        activity.logger.error(f"dbt run failed for {{model_name}}: {{error_msg}}")
        raise RuntimeError(f"dbt run --select {{model_name}} failed: {{error_msg}}")

    activity.logger.info(f"dbt model {{model_name}} completed successfully")
    return f"{{model_name}}: OK"


# All migrated models (for reference)
ALL_MODELS = {all_models!r}
'''
    act_path = orch_dir / "activities.py"
    act_path.write_text(activities_code, encoding="utf-8")
    files_created.append(str(act_path))

    # ── Write worker.py ───────────────────────────────
    root_workflow = _safe_class_name(project.roots[0]) + "Workflow" if project.roots else imported_classes[0]
    all_wf_imports = ", ".join(imported_classes)

    worker_code = f'''"""
Temporal Worker — registers workflows and activities, then starts polling.

Usage:
    python worker.py

Requires:
    - Temporal server running (temporal server start-dev)
    - dbt installed (pip install dbt-duckdb)
    - temporalio installed (pip install temporalio)
"""
import asyncio
from temporalio.client import Client
from temporalio.worker import Worker

from workflows import {all_wf_imports}
from activities import run_dbt_model

TASK_QUEUE = "taldbt-migration"
TEMPORAL_HOST = "localhost:7233"


async def main():
    client = await Client.connect(TEMPORAL_HOST)
    print(f"Connected to Temporal at {{TEMPORAL_HOST}}")

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[{all_wf_imports}],
        activities=[run_dbt_model],
    )

    print(f"Worker started on task queue: {{TASK_QUEUE}}")
    print(f"Registered workflows: {", ".join(imported_classes)}")
    print("Listening for tasks... (Ctrl+C to stop)")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
'''
    worker_path = orch_dir / "worker.py"
    worker_path.write_text(worker_code, encoding="utf-8")
    files_created.append(str(worker_path))

    # ── Write run_workflow.py ─────────────────────────
    root_name = project.roots[0] if project.roots else list(orch_jobs.keys())[0]

    run_code = f'''"""
Trigger the root Temporal workflow — runs the full migration pipeline.

Usage:
    python run_workflow.py

This connects to the local Temporal server, triggers {root_name},
and waits for completion.
"""
import asyncio
import uuid
from temporalio.client import Client

from workflows import {root_workflow}

TASK_QUEUE = "taldbt-migration"
TEMPORAL_HOST = "localhost:7233"


async def main():
    client = await Client.connect(TEMPORAL_HOST)
    print(f"Connected to Temporal at {{TEMPORAL_HOST}}")

    workflow_id = f"taldbt-{{uuid.uuid4().hex[:8]}}"
    print(f"Starting workflow: {root_workflow} (id={{workflow_id}})")

    result = await client.execute_workflow(
        {root_workflow}.run,
        id=workflow_id,
        task_queue=TASK_QUEUE,
    )

    print(f"\\nWorkflow completed: {{result}}")
    return result


if __name__ == "__main__":
    asyncio.run(main())
'''
    run_path = orch_dir / "run_workflow.py"
    run_path.write_text(run_code, encoding="utf-8")
    files_created.append(str(run_path))

    return {"files_created": files_created, "errors": errors}
