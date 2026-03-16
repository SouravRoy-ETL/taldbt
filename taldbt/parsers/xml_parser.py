"""
Core XML Parser: reads a single Talend .item file and produces a JobAST.
Delegates to component-specific parsers for deep extraction.

Handles both regular jobs (<talendfile:ProcessType> with <node>) and
Joblets (<model:JobletProcess> with <jobletNodes>).
"""
from pathlib import Path
from lxml import etree
import re

from taldbt.models.ast_models import (
    JobAST, JobType, ComponentAST, FlowConnection, TriggerType,
    ColumnSchema, ComponentBehavior,
)
from taldbt.parsers.classifier import classify
from taldbt.parsers.components.tmap_parser import parse_tmap
from taldbt.parsers.components.input_parser import parse_input_component
from taldbt.parsers.components.filter_parser import parse_filter
from taldbt.parsers.components.aggregate_parser import parse_aggregate
from taldbt.parsers.components.sort_parser import parse_sort
from taldbt.parsers.components.dedup_parser import parse_dedup


# ── Trigger type mapping ─────────────────────────────────

_TRIGGER_MAP = {
    "FLOW": TriggerType.FLOW,
    "MAIN": TriggerType.FLOW,
    "REJECT": TriggerType.REJECT,
    "FILTER": TriggerType.FILTER,
    "SUBJOB_OK": TriggerType.SUBJOB_OK,
    "SUBJOB_ERROR": TriggerType.SUBJOB_ERROR,
    "COMPONENT_OK": TriggerType.COMPONENT_OK,
    "COMPONENT_ERROR": TriggerType.COMPONENT_ERROR,
    "RUN_IF": TriggerType.RUN_IF,
    "PARALLELIZE": TriggerType.PARALLELIZE,
    "ITERATE": TriggerType.ITERATE,
}


def _local_tag(elem) -> str:
    """Strip namespace from element tag."""
    tag = elem.tag
    if "}" in tag:
        return tag.split("}")[1]
    return tag


def _get_params(node_elem) -> dict[str, str]:
    """Extract all elementParameter name→value from a node."""
    params = {}
    for child in node_elem:
        if "elementParameter" in _local_tag(child):
            name = child.get("name", "")
            value = child.get("value", "")
            if name:
                params[name] = value
    return params


def _get_unique_name(params: dict) -> str:
    return params.get("UNIQUE_NAME", "")


def _extract_schemas(node_elem) -> dict[str, list[ColumnSchema]]:
    """Extract <metadata> tags → column schemas per connector."""
    schemas = {}
    for child in node_elem:
        tag = _local_tag(child)
        if "metadata" in tag.lower() and child.get("connector"):
            connector = child.get("connector", "FLOW")
            cols = []
            ordinal = 0
            for col_elem in child:
                if "column" in _local_tag(col_elem).lower():
                    raw_len = int(col_elem.get("length", "0") or 0)
                    raw_prec = int(col_elem.get("precision", "0") or 0)
                    cs = ColumnSchema(
                        name=col_elem.get("name", ""),
                        talend_type=col_elem.get("type", "id_String"),
                        nullable=col_elem.get("nullable", "true").lower() == "true",
                        is_key=col_elem.get("key", "false").lower() == "true",
                        length=raw_len if raw_len > 0 else None,
                        precision=raw_prec if raw_prec > 0 else None,
                        comment=col_elem.get("comment", ""),
                        ordinal=ordinal,
                    )
                    cs.sql_type = cs.resolve_sql_type()
                    cols.append(cs)
                    ordinal += 1
            if cols:
                schemas[connector] = cols
    return schemas


def _extract_connections(root) -> list[FlowConnection]:
    """Extract all <connection> tags from the job."""
    connections = []
    for elem in root.iter():
        tag = _local_tag(elem)
        if tag == "connection":
            src = elem.get("source", "")
            tgt = elem.get("target", "")
            connector = elem.get("connectorName", "")
            flow_name = elem.get("label", "") or elem.get("name", "") or connector

            trigger = _TRIGGER_MAP.get(connector, TriggerType.UNKNOWN)
            if trigger == TriggerType.UNKNOWN and connector:
                trigger = TriggerType.FLOW

            condition = ""
            for child in elem:
                if "elementParameter" in _local_tag(child):
                    if child.get("name") == "CONDITION":
                        condition = child.get("value", "")

            if src and tgt:
                connections.append(FlowConnection(
                    source=src, target=tgt,
                    trigger_type=trigger,
                    flow_name=flow_name,
                    connector_name=connector,
                    condition=condition,
                ))
    return connections


def _extract_contexts(root) -> dict[str, dict[str, str]]:
    """Extract <context> groups with their parameters."""
    contexts = {}
    for elem in root.iter():
        tag = _local_tag(elem)
        if tag == "context":
            ctx_name = elem.get("name", "Default")
            params = {}
            for child in elem:
                if "contextParameter" in _local_tag(child):
                    pname = child.get("name", "")
                    pvalue = child.get("value", "")
                    if pname:
                        params[pname] = pvalue
            if params:
                contexts[ctx_name] = params
    return contexts


def _resolve_child_job(params: dict) -> str:
    """For tRunJob, resolve the child job name from PROCESS parameter."""
    raw = params.get("PROCESS", "") or params.get("PROCESS:PROCESS_TYPE_PROCESS", "")
    # Strip project prefix: "LOCAL_PROJECT:Job_Name" → "Job_Name"
    if ":" in raw and not raw.startswith("_"):
        raw = raw.split(":")[-1]
    if raw.startswith("_") or len(raw) > 30:
        raw = params.get("PROCESS", "")
        if ":" in raw:
            raw = raw.split(":")[-1]
    return raw


def _detect_joblet(root) -> bool:
    """Detect if this is a Joblet XML (vs regular job).

    Joblets use:
    - Root element: <model:JobletProcess> with xmlns:model="http://www.talend.com/joblet.ecore"
    - Component nodes: <jobletNodes> instead of <node>
    - Special nodes with input="true" or trigger="true"
    """
    root_tag = _local_tag(root)
    if root_tag in ("JobletProcess",):
        return True
    # Check namespace
    ns_map = root.nsmap if hasattr(root, 'nsmap') else {}
    for prefix, uri in ns_map.items():
        if "joblet" in uri.lower():
            return True
    return False


def _iter_component_nodes(root, is_joblet: bool):
    """Iterate over component nodes, handling both regular jobs and joblets.

    Regular jobs: <node componentName="tMap">
    Joblets: <jobletNodes componentName="tMap">
    """
    target_tags = {"node", "jobletNodes"}
    for elem in root.iter():
        tag = _local_tag(elem)
        if tag in target_tags:
            comp_name = elem.get("componentName", "")
            if comp_name:
                yield elem, comp_name


def parse_job(item_path: str, job_name: str = "", job_type: JobType = JobType.STANDARD) -> JobAST:
    """Parse a single Talend .item file into a JobAST."""
    parser = etree.XMLParser(recover=True, encoding="utf-8")
    tree = etree.parse(item_path, parser=parser)
    root = tree.getroot()

    # Auto-detect joblets
    is_joblet = _detect_joblet(root)
    if is_joblet:
        job_type = JobType.JOBLET

    job = JobAST(
        name=job_name or Path(item_path).stem,
        file_path=item_path,
        job_type=job_type,
    )

    # Extract contexts
    job.contexts = _extract_contexts(root)

    # Extract connections
    job.connections = _extract_connections(root)

    # Build flow name map: flow_label → source component unique name
    # This is THE key data structure for ref resolution in sql_generator.
    # When tMap has expr "workorderRouting.LocationID", this map tells us
    # workorderRouting = tDBInput_1's output flow.
    for conn in job.connections:
        if conn.trigger_type == TriggerType.FLOW and conn.flow_name:
            job.flow_name_map[conn.flow_name] = conn.source

    # Track whether job has any data-processing components
    has_orchestration_only = True

    # Extract components (handles both <node> and <jobletNodes>)
    for elem, comp_type in _iter_component_nodes(root, is_joblet):
        # Skip joblet trigger/input/output markers
        if comp_type in ("TRIGGER_INPUT", "TRIGGER_OUTPUT",
                         "tJobletInput", "tJobletOutput"):
            continue

        params = _get_params(elem)
        unique_name = _get_unique_name(params)
        if not unique_name:
            continue

        behavior = classify(comp_type)
        schemas = _extract_schemas(elem)

        # Preserve raw XML for LLM fallback (limited size)
        try:
            raw_xml = etree.tostring(elem, encoding="unicode", pretty_print=False)
            if len(raw_xml) > 50000:
                raw_xml = raw_xml[:50000] + "<!-- truncated -->"
        except Exception:
            raw_xml = ""

        comp = ComponentAST(
            unique_name=unique_name,
            component_type=comp_type,
            behavior=behavior,
            parameters=params,
            schemas=schemas,
            raw_xml=raw_xml,
        )

        # ── Deep parsing by behavior ─────────────────
        if behavior == ComponentBehavior.DATA_SOURCE:
            parse_input_component(comp, elem)
            has_orchestration_only = False

        elif behavior == ComponentBehavior.TRANSFORMER:
            parse_tmap(comp, elem)
            has_orchestration_only = False

        elif behavior == ComponentBehavior.FILTER:
            parse_filter(comp, elem)
            has_orchestration_only = False

        elif behavior == ComponentBehavior.AGGREGATE:
            parse_aggregate(comp, elem)
            has_orchestration_only = False

        elif behavior == ComponentBehavior.ORCHESTRATION:
            if comp_type == "tRunJob":
                comp.child_job_name = _resolve_child_job(params)
                job.child_jobs.append(comp.child_job_name)
            if comp_type == "tParallelize":
                job.has_parallelize = True

        elif behavior == ComponentBehavior.CUSTOM_CODE:
            # Extract Java code from CODE parameter
            comp.java_code = params.get("CODE", "")
            comp.confidence = 0.2  # needs LLM review
            has_orchestration_only = False

        elif behavior == ComponentBehavior.STATE_MANAGER:
            if comp_type == "tSetGlobalVar":
                for child in elem:
                    if "elementParameter" in _local_tag(child) and child.get("name") == "VARIABLES":
                        for ev in child:
                            key = ev.get("KEY", "")
                            value = ev.get("VALUE", "")
                            if key:
                                comp.global_var_entries[key] = value

        elif behavior == ComponentBehavior.DATA_SINK:
            parse_input_component(comp, elem)  # reuses same param extraction
            has_orchestration_only = False

        elif behavior in (ComponentBehavior.IGNORE, ComponentBehavior.OBSERVABILITY):
            pass  # no further parsing

        elif behavior == ComponentBehavior.DEDUP:
            parse_dedup(comp, elem)
            has_orchestration_only = False

        elif behavior == ComponentBehavior.SORT:
            parse_sort(comp, elem)
            has_orchestration_only = False

        elif behavior in (ComponentBehavior.UNION, ComponentBehavior.JOINER):
            has_orchestration_only = False

        else:
            has_orchestration_only = False

        job.components[unique_name] = comp

    # Auto-detect orchestration jobs
    if has_orchestration_only and job.child_jobs:
        job.job_type = JobType.ORCHESTRATION

    # Compute confidence
    _compute_confidence(job)

    return job


def _compute_confidence(job: JobAST):
    """Calculate what % of the job can be handled deterministically."""
    total = 0
    deterministic = 0
    needs_llm = []

    for comp in job.components.values():
        if comp.behavior in (ComponentBehavior.IGNORE, ComponentBehavior.OBSERVABILITY):
            continue
        total += 1
        if comp.behavior == ComponentBehavior.CUSTOM_CODE:
            needs_llm.append(f"{comp.unique_name}: {comp.component_type} (Java code)")
        elif comp.behavior == ComponentBehavior.AI_REQUIRED:
            needs_llm.append(f"{comp.unique_name}: {comp.component_type} (unknown)")
        elif comp.confidence < 0.5:
            needs_llm.append(f"{comp.unique_name}: low confidence ({comp.confidence:.0%})")
        else:
            deterministic += 1

    job.deterministic_pct = (deterministic / total * 100) if total > 0 else 100
    job.needs_llm = needs_llm
