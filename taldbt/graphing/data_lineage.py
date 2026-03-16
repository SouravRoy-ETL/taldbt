"""
Data Lineage Analyzer: discovers cross-job data dependencies by tracing
which tables each job READS from (via tDBInput/tFileInput queries) and
WRITES to (via tDBOutput/tMysqlOutput TABLE parameters).

This catches the case where:
  - Job A (orchestration-orphan) writes to table "dim_scarpreason"
  - Job B (orchestration-connected) reads from table "dim_scarpreason"
  → Job A is data-dependent and MUST be migrated even though no tRunJob calls it.

Uses transitive closure: if orphan A writes X, orphan B reads X and writes Y,
connected job reads Y → both A and B are data-dependent.
"""
from __future__ import annotations
import re
from taldbt.models.ast_models import (
    ProjectAST, JobAST, ComponentBehavior,
)


def _clean_table_name(raw: str) -> str:
    """Normalize a table name for matching: strip quotes, backticks, whitespace, lowercase."""
    t = raw.strip().strip('"').strip("'").strip('`').strip()
    # Remove schema prefix for matching (Schema.Table → Table)
    # But keep the last segment as the table name
    parts = t.replace('`', '').split('.')
    # Use the last part as the canonical table name
    table = parts[-1].strip().lower()
    return table


def _extract_tables_from_sql(sql: str) -> set[str]:
    """Extract table names from a SQL query string (FROM / JOIN clauses)."""
    tables = set()
    if not sql:
        return tables

    # Clean XML entities first
    cleaned = sql.replace("&#10;", "\n").replace("&#13;", "").replace("&#9;", " ")
    cleaned = cleaned.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    cleaned = cleaned.replace("\\n", "\n").replace("\\t", " ")
    cleaned = cleaned.replace("`", "")

    # FROM table, JOIN table patterns
    # Matches: FROM schema.table, FROM table, JOIN schema.table
    for match in re.finditer(
        r'(?:FROM|JOIN)\s+([a-zA-Z_][\w.]*)',
        cleaned,
        re.IGNORECASE,
    ):
        raw_table = match.group(1)
        table = _clean_table_name(raw_table)
        if table and table not in ('dual', 'select', 'where', 'and', 'or'):
            tables.add(table)

    return tables


def analyze_job_lineage(job: JobAST) -> tuple[set[str], set[str]]:
    """Analyze a single job to find which tables it reads and writes.

    Returns: (reads: set of table names, writes: set of table names)
    """
    reads = set()
    writes = set()

    for comp in job.components.values():
        behavior = comp.behavior
        params = comp.parameters

        if behavior == ComponentBehavior.DATA_SOURCE:
            # Extract READ tables from SQL query
            query = params.get("QUERY", "")
            if query:
                reads.update(_extract_tables_from_sql(query))

            # Also grab TABLE parameter as fallback
            table = params.get("TABLE", "")
            if table:
                t = _clean_table_name(table)
                if t:
                    reads.add(t)

        elif behavior == ComponentBehavior.DATA_SINK:
            # Extract WRITE table
            table = params.get("TABLE", "")
            if table:
                t = _clean_table_name(table)
                if t:
                    writes.add(t)

        elif behavior == ComponentBehavior.SQL_EXEC:
            # tDBRow can both read and write
            query = params.get("QUERY", "")
            if query:
                reads.update(_extract_tables_from_sql(query))
            table = params.get("TABLE", "")
            if table:
                t = _clean_table_name(table)
                if t:
                    reads.add(t)

    return reads, writes


def build_data_lineage(project: ProjectAST) -> dict:
    """Build complete data lineage map across all jobs.

    Returns dict with:
      - job_reads: {job_name: set of table names it reads}
      - job_writes: {job_name: set of table names it writes}
      - table_writers: {table_name: set of job names that write it}
      - table_readers: {table_name: set of job names that read it}
      - data_edges: [(writer_job, reader_job, table_name), ...]
    """
    job_reads: dict[str, set[str]] = {}
    job_writes: dict[str, set[str]] = {}
    table_writers: dict[str, set[str]] = {}
    table_readers: dict[str, set[str]] = {}

    # Phase 1: Scan every job for reads/writes
    for name, job in project.jobs.items():
        reads, writes = analyze_job_lineage(job)
        job_reads[name] = reads
        job_writes[name] = writes

        for table in reads:
            table_readers.setdefault(table, set()).add(name)
        for table in writes:
            table_writers.setdefault(table, set()).add(name)

    # Phase 2: Find cross-job data edges (writer → reader via shared table)
    data_edges = []
    for table, writers in table_writers.items():
        readers = table_readers.get(table, set())
        for writer in writers:
            for reader in readers:
                if writer != reader:
                    data_edges.append((writer, reader, table))

    return {
        "job_reads": job_reads,
        "job_writes": job_writes,
        "table_writers": table_writers,
        "table_readers": table_readers,
        "data_edges": data_edges,
    }


def classify_orphans(
    project: ProjectAST,
    orchestration_connected: set[str],
    orchestration_orphans: set[str],
    lineage: dict,
) -> tuple[list[str], list[str]]:
    """Classify orchestration-orphan jobs into data-dependent vs truly dead.

    A data-dependent orphan is one where:
      - It writes to a table that a connected job reads from, OR
      - It writes to a table that another data-dependent orphan reads from
        (transitive closure)

    Returns: (data_dependent_jobs, dead_jobs)
    """
    job_reads = lineage["job_reads"]
    job_writes = lineage["job_writes"]
    table_writers = lineage["table_writers"]
    table_readers = lineage["table_readers"]

    # Start with the orchestration-connected set
    needed = set(orchestration_connected)
    data_dependent = set()

    # Iterative expansion: keep finding orphans whose output is needed
    changed = True
    while changed:
        changed = False
        for orphan in orchestration_orphans:
            if orphan in needed or orphan in data_dependent:
                continue

            # Check if any table this orphan writes to is read by a needed/connected job
            writes = job_writes.get(orphan, set())
            for table in writes:
                readers = table_readers.get(table, set())
                # If any reader is in needed set or already data-dependent
                if readers & (needed | data_dependent):
                    data_dependent.add(orphan)
                    changed = True
                    break

    dead = orchestration_orphans - data_dependent
    return sorted(data_dependent), sorted(dead)


def apply_lineage_to_project(project: ProjectAST):
    """Run full lineage analysis and store results in the ProjectAST.

    Must be called AFTER apply_dag_to_project() since it uses
    connected_jobs and orphan_jobs.
    """
    lineage = build_data_lineage(project)

    # Store per-job lineage
    for name, job in project.jobs.items():
        job.reads_tables = sorted(lineage["job_reads"].get(name, set()))
        job.writes_tables = sorted(lineage["job_writes"].get(name, set()))

    # Store cross-job data edges
    project.data_lineage_edges = [
        {"writer": w, "reader": r, "table": t}
        for w, r, t in lineage["data_edges"]
    ]

    # Classify orphans
    connected_set = set(project.connected_jobs)
    orphan_set = set(project.orphan_jobs)

    data_dependent, dead = classify_orphans(
        project, connected_set, orphan_set, lineage,
    )

    project.data_dependent_jobs = data_dependent
    project.dead_jobs = dead

    # Update orphan_jobs to only be truly dead ones
    # connected_jobs now includes data-dependent
    project.connected_jobs = sorted(connected_set | set(data_dependent))
    project.orphan_jobs = dead
