"""
Job DAG Builder: builds inter-job dependency graph from tRunJob/tParallelize/OnSubjobOk.
Component DAG Builder: builds intra-job execution order from FLOW connections.
DAG Validator: detects cycles, orphans, roots.
"""
from __future__ import annotations
import re
import networkx as nx
from taldbt.models.ast_models import (
    ProjectAST, JobAST, TriggerType, ComponentBehavior,
)


def _normalize_name(name: str) -> str:
    """Normalize job name for matching: strip version, lowercase, remove non-alnum."""
    name = re.sub(r'_\d+\.\d+$', '', name)
    return re.sub(r'[^a-z0-9]', '', name.lower())


def _find_job_by_child_name(child_ref: str, all_jobs: dict[str, JobAST]) -> str | None:
    """Resolve a tRunJob child reference to an actual job name in the project.
    Handles mismatches like 'JobName' vs 'JobName_0.1' (version suffix)."""
    if not child_ref:
        return None
    # Direct match
    if child_ref in all_jobs:
        return child_ref
    # Normalized match
    norm = _normalize_name(child_ref)
    for name in all_jobs:
        if _normalize_name(name) == norm:
            return name
    return None


def build_job_dag(project: ProjectAST) -> nx.DiGraph:
    """Build the inter-job dependency graph."""
    G = nx.DiGraph()

    # Add all jobs as nodes
    for name, job in project.jobs.items():
        G.add_node(name, job_type=job.job_type.value)

    # For each job, find tRunJob references and add parent→child edges
    for parent_name, job in project.jobs.items():
        # Collect all child job references from tRunJob components
        child_refs = []
        for comp in job.components.values():
            if comp.child_job_name:
                resolved = _find_job_by_child_name(comp.child_job_name, project.jobs)
                if resolved and resolved != parent_name:
                    child_refs.append((comp.unique_name, resolved))
                    # Add edge: parent orchestration job → child job
                    G.add_edge(parent_name, resolved, trigger="tRunJob")

        # Now handle SUBJOB_OK connections BETWEEN tRunJob components
        # This creates sequential dependencies between child jobs
        for conn in job.connections:
            if conn.trigger_type == TriggerType.SUBJOB_OK:
                src_comp = job.components.get(conn.source)
                tgt_comp = job.components.get(conn.target)
                if not src_comp or not tgt_comp:
                    continue

                # If source is a tRunJob, get its child
                src_child = None
                if src_comp.child_job_name:
                    src_child = _find_job_by_child_name(src_comp.child_job_name, project.jobs)

                # If target is a tRunJob, get its child
                tgt_child = None
                if tgt_comp.child_job_name:
                    tgt_child = _find_job_by_child_name(tgt_comp.child_job_name, project.jobs)

                # If target is tParallelize, find all tRunJobs it connects to
                if tgt_comp.component_type == "tParallelize":
                    for pconn in job.connections:
                        if pconn.trigger_type == TriggerType.PARALLELIZE and pconn.source == tgt_comp.unique_name:
                            par_tgt = job.components.get(pconn.target)
                            if par_tgt and par_tgt.child_job_name:
                                par_child = _find_job_by_child_name(par_tgt.child_job_name, project.jobs)
                                if src_child and par_child and src_child != par_child:
                                    G.add_edge(src_child, par_child, trigger="SUBJOB_OK→PARALLELIZE")

                # Direct sequential: tRunJob_A → OnSubjobOk → tRunJob_B
                if src_child and tgt_child and src_child != tgt_child:
                    G.add_edge(src_child, tgt_child, trigger="SUBJOB_OK")

    return G


def validate_dag(G: nx.DiGraph) -> dict:
    """Validate the job DAG."""
    is_dag = nx.is_directed_acyclic_graph(G)
    cycles = list(nx.simple_cycles(G)) if not is_dag else []
    roots = [n for n, d in G.in_degree() if d == 0]
    leaves = [n for n, d in G.out_degree() if d == 0]

    # Orphans: nodes with NO connections at all (in or out)
    orphans = [n for n in G.nodes if G.degree(n) == 0]

    # True roots: orchestration jobs that have children but no parents
    true_roots = [n for n in roots if G.out_degree(n) > 0]
    # If no true roots found, use all roots that are orchestration type
    if not true_roots:
        true_roots = [n for n in roots if G.nodes[n].get("job_type") == "ORCHESTRATION"]

    build_order = []
    if is_dag:
        try:
            build_order = list(nx.topological_sort(G))
        except:
            build_order = list(G.nodes)

    return {
        "is_valid": is_dag,
        "cycles": cycles,
        "roots": true_roots if true_roots else roots,
        "all_roots": roots,
        "leaves": leaves,
        "orphans": orphans,
        "build_order": build_order,
        "max_depth": _max_depth(G, roots),
        "total_jobs": len(G.nodes),
    }


def _max_depth(G: nx.DiGraph, roots: list[str]) -> int:
    max_d = 0
    for r in roots:
        try:
            lengths = nx.single_source_shortest_path_length(G, r)
            max_d = max(max_d, max(lengths.values()) if lengths else 0)
        except:
            pass
    return max_d


def build_component_dag(job: JobAST) -> list[str]:
    """Build intra-job execution order from FLOW connections."""
    G = nx.DiGraph()
    for conn in job.connections:
        if conn.trigger_type in (TriggerType.FLOW, TriggerType.REJECT, TriggerType.FILTER):
            G.add_edge(conn.source, conn.target)

    try:
        order = list(nx.topological_sort(G))
    except nx.NetworkXUnfeasible:
        order = list(job.components.keys())

    return [n for n in order if n in job.components]


def apply_dag_to_project(project: ProjectAST):
    """Compute and store DAG info in the ProjectAST."""
    G = build_job_dag(project)
    result = validate_dag(G)

    project.roots = result["roots"]
    project.build_order = result["build_order"]
    project.job_dag_edges = [
        {"from": u, "to": v, "trigger": d.get("trigger", "DEPENDENCY")}
        for u, v, d in G.edges(data=True)
    ]

    # Identify parallel groups from tParallelize
    parallel_groups = []
    for name, job in project.jobs.items():
        if job.has_parallelize:
            group = []
            for comp in job.components.values():
                if comp.child_job_name:
                    resolved = _find_job_by_child_name(comp.child_job_name, project.jobs)
                    if resolved:
                        group.append(resolved)
            if group:
                parallel_groups.append(group)
    project.parallel_groups = parallel_groups

    # Compute connected vs orphan jobs
    # Connected = any job that has at least one edge (called by someone OR calls someone)
    connected = set()
    for u, v, _ in G.edges(data=True):
        connected.add(u)
        connected.add(v)
    # Also include all roots (even if they have no children, they're entry points)
    connected.update(result["roots"])
    project.connected_jobs = sorted(connected)
    project.orphan_jobs = sorted(set(project.jobs.keys()) - connected)

    # Build component execution order for each job
    for name, job in project.jobs.items():
        job.execution_order = build_component_dag(job)

    # Phase 2: Data lineage — find cross-job table dependencies
    # This reclassifies orchestration-orphans that are needed for data
    from taldbt.graphing.data_lineage import apply_lineage_to_project
    apply_lineage_to_project(project)
