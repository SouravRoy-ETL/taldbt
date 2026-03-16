"""
Job Analyzer: reads a full JobAST and produces a structured migration brief.

This is what a senior developer does when they open a Talend job:
  1. Identify all sources, transforms, sinks
  2. Trace the data flow (what feeds what)
  3. Read every expression, every join key, every filter
  4. Understand the business logic (what is this job doing?)
  5. Note the source SQL dialect
  6. Identify what needs human review

The output is a MigrationBrief — a complete, structured understanding of the job
that the SQL constructor or LLM can use to generate correct dbt SQL.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional
import networkx as nx

from taldbt.models.ast_models import (
    JobAST, ComponentAST, FlowConnection, TriggerType,
    ComponentBehavior, SourceType, ExpressionStrategy,
)
from taldbt.expert.component_kb import lookup, is_data_component, should_skip, needs_temporal


# ═══════════════════════════════════════════════════════════
# Migration Brief — the structured output of job analysis
# ═══════════════════════════════════════════════════════════

@dataclass
class SourceBrief:
    """One data source in the job."""
    cte_name: str           # tDBInput_1
    component_type: str     # tMysqlInput
    role: str               # data_source_db, data_source_file, etc.
    dialect: str            # mysql, oracle, bigquery...
    table_name: str = ""    # cleaned table name (no schema prefix)
    schema_name: str = ""   # schema if present
    database_name: str = "" # source database name (matches sources.yml)
    query_sql: str = ""     # cleaned embedded SQL (if any)
    file_path: str = ""     # for file sources
    file_format: str = ""   # csv, json, parquet, excel
    columns: list[str] = field(default_factory=list)
    needs_temporal: bool = False  # API/SaaS sources → Temporal activity


@dataclass
class TransformBrief:
    """One transformation in the job (tMap, tFilter, etc.)."""
    cte_name: str
    component_type: str
    role: str               # transformer, filter, aggregate, sort, dedup, etc.
    input_flows: list[str] = field(default_factory=list)  # flow names feeding this
    input_ctes: list[str] = field(default_factory=list)   # resolved CTE names

    # tMap-specific
    main_input: str = ""
    join_clauses: list[dict] = field(default_factory=list)  # [{lookup_cte, join_type, on_keys}]
    select_expressions: list[dict] = field(default_factory=list)  # [{col, expr, strategy}]
    where_filter: str = ""
    var_expressions: list[dict] = field(default_factory=list)  # [{name, expr}]

    # Other transforms
    filter_conditions: list[str] = field(default_factory=list)
    group_by_cols: list[str] = field(default_factory=list)
    aggregate_funcs: list[dict] = field(default_factory=list)
    sort_cols: list[dict] = field(default_factory=list)
    dedup_keys: list[str] = field(default_factory=list)

    # Custom code
    java_code: str = ""
    needs_llm: bool = False


@dataclass
class SinkBrief:
    """One data sink in the job."""
    component_type: str
    role: str
    table_name: str = ""
    input_cte: str = ""     # what CTE feeds this sink


@dataclass
class MigrationBrief:
    """Complete understanding of a job — everything needed to generate dbt SQL."""
    job_name: str
    job_purpose: str = ""   # human-readable description of what this job does

    # Data flow
    sources: list[SourceBrief] = field(default_factory=list)
    transforms: list[TransformBrief] = field(default_factory=list)
    sinks: list[SinkBrief] = field(default_factory=list)
    execution_order: list[str] = field(default_factory=list)  # topological CTE order

    # Flow resolution
    flow_to_cte: dict[str, str] = field(default_factory=dict)  # flow_label → cte_name

    # Source dialect (most common among inputs)
    primary_dialect: str = "mysql"

    # dbt model config
    materialization: str = "view"  # view, table, incremental, snapshot
    dbt_layer: str = "staging"     # staging, intermediate, marts

    # Flags
    needs_llm: bool = False
    has_custom_code: bool = False
    has_api_sources: bool = False
    has_scd: bool = False
    confidence: float = 1.0
    warnings: list[str] = field(default_factory=list)
    skipped_components: list[str] = field(default_factory=list)


# ═══════════════════════════════════════════════════════════
# Analyzer
# ═══════════════════════════════════════════════════════════

def analyze_job(job: JobAST) -> MigrationBrief:
    """Analyze a job and produce a complete migration brief.

    This is the core intelligence — understanding what the job does.
    """
    brief = MigrationBrief(job_name=job.name)

    # ── Build flow graph ──────────────────────────────
    G = nx.DiGraph()
    for conn in job.connections:
        if conn.trigger_type in (TriggerType.FLOW, TriggerType.REJECT, TriggerType.FILTER):
            G.add_edge(conn.source, conn.target, flow_name=conn.flow_name)

    # ── Flow resolution map ───────────────────────────
    # This is THE key insight: flow_label → source CTE name
    brief.flow_to_cte = dict(job.flow_name_map)  # already built by xml_parser

    # ── Topological order ─────────────────────────────
    try:
        order = list(nx.topological_sort(G))
    except nx.NetworkXUnfeasible:
        order = list(job.components.keys())
    brief.execution_order = [n for n in order if n in job.components]

    # ── Analyze each component ────────────────────────
    dialects = []

    for comp_name in brief.execution_order:
        comp = job.components[comp_name]
        kb = lookup(comp.component_type)

        # Skip infrastructure components
        if should_skip(comp.component_type):
            brief.skipped_components.append(f"{comp_name} ({comp.component_type}): {kb.purpose}")
            continue

        # Get upstream CTEs
        predecessors = list(G.predecessors(comp_name)) if comp_name in G else []

        # ── DATA SOURCES ──────────────────────────────
        if kb.role.startswith("data_source"):
            src = SourceBrief(
                cte_name=comp_name,
                component_type=comp.component_type,
                role=kb.role,
                dialect=kb.dialect,
            )

            if kb.dialect:
                dialects.append(kb.dialect)

            si = comp.source_info
            if si:
                if si.connection and si.connection.table:
                    raw_table = si.connection.table.replace('"', '').replace("'", "")
                    parts = raw_table.split(".")
                    src.table_name = parts[-1]
                    src.schema_name = parts[0] if len(parts) > 1 else ""

                if si.connection and si.connection.database:
                    db = si.connection.database.replace('"', '').replace("'", "")
                    src.database_name = db.replace("-", "_").replace(".", "_")

                if si.query and si.query.cleaned_sql:
                    src.query_sql = si.query.cleaned_sql

                if si.file_path:
                    src.file_path = si.file_path

                if si.source_type == SourceType.FILE_CSV:
                    src.file_format = "csv"
                elif si.source_type == SourceType.FILE_EXCEL:
                    src.file_format = "excel"
                elif si.source_type == SourceType.FILE_JSON:
                    src.file_format = "json"
                elif si.source_type == SourceType.FILE_PARQUET:
                    src.file_format = "parquet"
                elif si.source_type == SourceType.FILE_XML:
                    src.file_format = "xml"

                src.columns = [c.name for c in si.columns] if si.columns else []

            if needs_temporal(comp.component_type):
                src.needs_temporal = True
                brief.has_api_sources = True

            brief.sources.append(src)

        # ── TRANSFORMERS ──────────────────────────────
        elif kb.role in ("transformer", "filter", "aggregate", "sort", "dedup",
                          "join", "union", "normalize", "denormalize", "pivot",
                          "unpivot", "custom_code"):

            tx = TransformBrief(
                cte_name=comp_name,
                component_type=comp.component_type,
                role=kb.role,
                input_ctes=predecessors,
            )

            # ── tMap deep analysis ────────────────────
            if comp.tmap_data and comp.tmap_data.inputs:
                td = comp.tmap_data

                # Main input
                for inp in td.inputs:
                    if inp.is_main_input:
                        tx.main_input = brief.flow_to_cte.get(inp.name, inp.name)
                        break
                if not tx.main_input and td.inputs:
                    tx.main_input = brief.flow_to_cte.get(td.inputs[0].name, td.inputs[0].name)

                # Lookups/Joins
                for inp in td.inputs:
                    if not inp.is_main_input:
                        lookup_cte = brief.flow_to_cte.get(inp.name, inp.name)
                        on_keys = []
                        for jk in inp.join_keys:
                            on_keys.append({"lookup_col": jk.name, "expr": jk.expression})
                        tx.join_clauses.append({
                            "lookup_cte": lookup_cte,
                            "join_type": inp.join_type,
                            "matching_mode": inp.matching_mode,
                            "on_keys": on_keys,
                        })

                # Output expressions
                for out in td.outputs:
                    if out.is_reject:
                        continue
                    for col in out.columns:
                        tx.select_expressions.append({
                            "column": col.column_name,
                            "expression": col.expression or "",
                            "strategy": col.strategy.value,
                        })
                        if col.strategy == ExpressionStrategy.LLM_REQUIRED:
                            tx.needs_llm = True

                    if out.filter and out.filter.expression:
                        tx.where_filter = out.filter.expression
                    break  # first non-reject output

                # Var table
                for v in td.var_table:
                    tx.var_expressions.append({"name": v.name, "expr": v.expression})

            # ── Filter conditions ─────────────────────
            if comp.filter_conditions:
                for fc in comp.filter_conditions:
                    tx.filter_conditions.append(fc.expression if hasattr(fc, 'expression') and fc.expression
                                                 else f"{fc.input_column} {fc.function} {fc.value}")

            # ── Aggregate config ──────────────────────
            if comp.aggregate_config:
                ac = comp.aggregate_config
                tx.group_by_cols = list(ac.group_by) if ac.group_by else []
                for op in ac.operations:
                    tx.aggregate_funcs.append({
                        "output": op.output_column,
                        "func": op.function,
                        "input": op.input_column,
                    })

            # ── Sort config ───────────────────────────
            if comp.sort_config:
                for i, col in enumerate(comp.sort_config.columns):
                    order = comp.sort_config.orders[i] if i < len(comp.sort_config.orders) else "ASC"
                    tx.sort_cols.append({"column": col, "order": order})

            # ── Dedup config ──────────────────────────
            if comp.unique_config:
                tx.dedup_keys = list(comp.unique_config.key_columns)

            # ── Custom code ───────────────────────────
            if kb.role == "custom_code":
                tx.java_code = comp.java_code or ""
                tx.needs_llm = True
                brief.has_custom_code = True

            if tx.needs_llm:
                brief.needs_llm = True

            brief.transforms.append(tx)

        # ── DATA SINKS ────────────────────────────────
        elif kb.role.startswith("data_sink"):
            sink = SinkBrief(
                component_type=comp.component_type,
                role=kb.role,
                table_name=comp.parameters.get("TABLE", "").replace('"', '').replace("'", ""),
                input_cte=predecessors[0] if predecessors else "",
            )
            brief.sinks.append(sink)

        # ── SCD ───────────────────────────────────────
        elif kb.role == "scd":
            brief.has_scd = True
            brief.materialization = "snapshot"

        # ── Everything else → warning ─────────────────
        elif kb.role in ("unknown",):
            brief.warnings.append(f"{comp_name} ({comp.component_type}): unknown component, needs LLM analysis")
            brief.needs_llm = True

    # ── Determine primary dialect ─────────────────────
    if dialects:
        from collections import Counter
        brief.primary_dialect = Counter(dialects).most_common(1)[0][0]

    # ── Determine dbt layer ───────────────────────────
    name_lower = job.name.lower()
    if any(name_lower.startswith(p) for p in ("stg_", "staging_", "src_", "load_")):
        brief.dbt_layer = "staging"
    elif any(name_lower.startswith(p) for p in ("int_", "intermediate_", "tmp_")):
        brief.dbt_layer = "intermediate"
    elif any(kw in name_lower for kw in ("dim", "fact", "bridge")):
        brief.dbt_layer = "staging"  # dim/fact transforms are still staging
    else:
        brief.dbt_layer = "marts"

    # ── Determine materialization ─────────────────────
    if brief.has_scd:
        brief.materialization = "snapshot"
    elif any(s.role == "data_sink_db" for s in brief.sinks):
        brief.materialization = "table"
    else:
        brief.materialization = "view"

    # ── Compute confidence ────────────────────────────
    total_exprs = sum(len(t.select_expressions) for t in brief.transforms)
    llm_exprs = sum(1 for t in brief.transforms for e in t.select_expressions
                    if e["strategy"] == "LLM_REQUIRED")
    if total_exprs > 0:
        brief.confidence = 1.0 - (llm_exprs / total_exprs)

    # ── Generate job purpose description ──────────────
    src_tables = [s.table_name or s.file_path for s in brief.sources if s.table_name or s.file_path]
    sink_tables = [s.table_name for s in brief.sinks if s.table_name]
    transform_types = [t.role for t in brief.transforms]

    brief.job_purpose = (
        f"Reads from {', '.join(src_tables[:3]) or 'unknown sources'}, "
        f"applies {', '.join(set(transform_types)) or 'passthrough'}, "
        f"writes to {', '.join(sink_tables[:2]) or 'output flow'}"
    )

    return brief
