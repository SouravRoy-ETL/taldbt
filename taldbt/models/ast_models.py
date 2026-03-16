"""
Core AST data models for taldbt.
Every parser writes into these structures. Every code generator reads from them.
This is the contract between parsing and generation — the Intermediate Representation.
"""
from __future__ import annotations
from pydantic import BaseModel, Field
from enum import Enum
from typing import Optional


# ── Enums ───────────────────────────────────────────────

class ComponentBehavior(str, Enum):
    DATA_SOURCE = "DATA_SOURCE"
    DATA_SINK = "DATA_SINK"
    TRANSFORMER = "TRANSFORMER"
    FILTER = "FILTER"
    AGGREGATE = "AGGREGATE"
    DEDUP = "DEDUP"
    SORT = "SORT"
    JOINER = "JOINER"
    UNION = "UNION"
    PIVOT = "PIVOT"
    UNPIVOT = "UNPIVOT"
    PROJECTION = "PROJECTION"
    TRANSFORM_SIMPLE = "TRANSFORM_SIMPLE"
    CUSTOM_CODE = "CUSTOM_CODE"
    SQL_EXEC = "SQL_EXEC"
    SCD_HANDLER = "SCD_HANDLER"
    STATE_MANAGER = "STATE_MANAGER"
    ORCHESTRATION = "ORCHESTRATION"
    OBSERVABILITY = "OBSERVABILITY"
    FILE_FETCH = "FILE_FETCH"
    FILE_PUSH = "FILE_PUSH"
    ITERATOR = "ITERATOR"
    SIDE_EFFECT = "SIDE_EFFECT"
    DDL = "DDL"
    IGNORE = "IGNORE"
    AI_REQUIRED = "AI_REQUIRED"


class SourceType(str, Enum):
    DATABASE = "DATABASE"
    FILE_CSV = "FILE_CSV"
    FILE_JSON = "FILE_JSON"
    FILE_XML = "FILE_XML"
    FILE_EXCEL = "FILE_EXCEL"
    FILE_PARQUET = "FILE_PARQUET"
    API_REST = "API_REST"
    API_SOAP = "API_SOAP"
    SAAS = "SAAS"
    UNKNOWN = "UNKNOWN"


class JobType(str, Enum):
    STANDARD = "STANDARD"
    ORCHESTRATION = "ORCHESTRATION"
    JOBLET = "JOBLET"


class TriggerType(str, Enum):
    FLOW = "FLOW"
    REJECT = "REJECT"
    FILTER = "FILTER"
    SUBJOB_OK = "SUBJOB_OK"
    SUBJOB_ERROR = "SUBJOB_ERROR"
    COMPONENT_OK = "COMPONENT_OK"
    COMPONENT_ERROR = "COMPONENT_ERROR"
    RUN_IF = "RUN_IF"
    PARALLELIZE = "PARALLELIZE"
    ITERATE = "ITERATE"
    UNKNOWN = "UNKNOWN"


class DbtStrategy(str, Enum):
    SOURCE = "source"
    SEED = "seed"
    EXTERNAL_TABLE = "external_table"
    TEMPORAL_ACTIVITY = "temporal_activity"
    REF = "ref"


class ExpressionStrategy(str, Enum):
    DETERMINISTIC = "DETERMINISTIC"          # simple column ref, arithmetic
    KNOWLEDGE_BASE = "KNOWLEDGE_BASE"        # known Talend routine → SQL mapping
    LLM_REQUIRED = "LLM_REQUIRED"           # unknown Java, custom routines


# ── Column & Schema ─────────────────────────────────────

TALEND_TYPE_MAP = {
    "id_Integer": "INTEGER", "id_Long": "BIGINT", "id_Float": "FLOAT",
    "id_Double": "DOUBLE", "id_String": "VARCHAR", "id_Boolean": "BOOLEAN",
    "id_Date": "DATE", "id_Byte": "TINYINT", "id_Short": "SMALLINT",
    "id_BigDecimal": "DECIMAL", "id_byte[]": "BLOB", "id_Character": "CHAR(1)",
    "id_Object": "VARCHAR", "id_List": "VARCHAR", "id_Document": "JSON",
}


class ColumnSchema(BaseModel):
    name: str
    talend_type: str = ""
    sql_type: str = "VARCHAR"
    nullable: bool = True
    is_key: bool = False
    length: Optional[int] = None
    precision: Optional[int] = None
    comment: str = ""
    ordinal: int = 0

    def resolve_sql_type(self) -> str:
        base = TALEND_TYPE_MAP.get(self.talend_type, "VARCHAR")
        if base == "DECIMAL" and self.length and self.length > 0 and self.precision:
            return f"DECIMAL({self.length},{self.precision})"
        if base == "VARCHAR" and self.length and self.length > 0:
            return f"VARCHAR({self.length})"
        return base


# ── Source & Connection ──────────────────────────────────

class ConnectionInfo(BaseModel):
    host: str = ""
    port: str = ""
    database: str = ""
    schema_name: str = ""
    table: str = ""
    db_type: str = ""
    username: str = ""
    properties: str = ""


class EmbeddedQuery(BaseModel):
    raw_sql: str
    cleaned_sql: str = ""
    source_tables: list[str] = Field(default_factory=list)
    has_context_vars: bool = False
    context_var_refs: list[str] = Field(default_factory=list)


class SourceInfo(BaseModel):
    source_type: SourceType = SourceType.UNKNOWN
    connection: Optional[ConnectionInfo] = None
    query: Optional[EmbeddedQuery] = None
    file_path: str = ""
    file_format: str = ""
    delimiter: str = ""
    has_header: bool = True
    encoding: str = "UTF-8"
    url: str = ""
    method: str = ""
    columns: list[ColumnSchema] = Field(default_factory=list)
    dbt_strategy: DbtStrategy = DbtStrategy.SOURCE

    @property
    def source_id(self) -> str:
        if self.connection and self.connection.table:
            db = self.connection.database.replace('"', '').replace("'", "")
            tbl = self.connection.table.replace('"', '').replace("'", "")
            return f"{db}__{tbl}" if db else tbl
        if self.file_path:
            return self.file_path.replace("/", "_").replace("\\", "_").replace(".", "_")
        return "unknown_source"


# ── tMap Structures ──────────────────────────────────────

class TMapJoinKey(BaseModel):
    name: str              # column name in the lookup table
    expression: str        # e.g. "row1.ADDRESSID"


class TMapInput(BaseModel):
    name: str              # flow name: "row1", "row2"
    matching_mode: str = "ALL_ROWS"  # ALL_ROWS, FIRST_MATCH, ALL_MATCHES
    join_type: str = "INNER"         # INNER, LEFT_OUTER, CROSS
    join_keys: list[TMapJoinKey] = Field(default_factory=list)
    is_main_input: bool = False      # first input = FROM table


class TMapExpression(BaseModel):
    column_name: str
    expression: str
    expression_type: str = ""    # Talend type of the output
    strategy: ExpressionStrategy = ExpressionStrategy.DETERMINISTIC
    translated_sql: str = ""     # filled by expression classifier


class TMapOutputFilter(BaseModel):
    expression: str
    strategy: ExpressionStrategy = ExpressionStrategy.DETERMINISTIC
    translated_sql: str = ""


class TMapOutput(BaseModel):
    name: str
    columns: list[TMapExpression] = Field(default_factory=list)
    filter: Optional[TMapOutputFilter] = None
    is_reject: bool = False


class TMapVarEntry(BaseModel):
    name: str
    expression: str
    type: str = ""


class TMapData(BaseModel):
    inputs: list[TMapInput] = Field(default_factory=list)
    outputs: list[TMapOutput] = Field(default_factory=list)
    var_table: list[TMapVarEntry] = Field(default_factory=list)


# ── Filter / Aggregate / Unique Structures ───────────────

class FilterCondition(BaseModel):
    input_column: str
    function: str          # ==, !=, <, >, CONTAINS, MATCHES, etc.
    value: str
    logical_op: str = "AND"


class AggregateOperation(BaseModel):
    output_column: str
    function: str          # SUM, COUNT, MIN, MAX, AVG, FIRST, LAST, LIST
    input_column: str


class AggregateConfig(BaseModel):
    group_by: list[str] = Field(default_factory=list)
    operations: list[AggregateOperation] = Field(default_factory=list)


class UniqueConfig(BaseModel):
    key_columns: list[str] = Field(default_factory=list)
    case_sensitive: bool = True


class SortConfig(BaseModel):
    columns: list[str] = Field(default_factory=list)
    orders: list[str] = Field(default_factory=list)  # ASC / DESC


# ── SCD Config ───────────────────────────────────────────

class SCDConfig(BaseModel):
    scd_type: int = 2           # 1 or 2
    business_keys: list[str] = Field(default_factory=list)
    tracked_columns: list[str] = Field(default_factory=list)
    start_date_col: str = ""
    end_date_col: str = ""
    current_flag_col: str = ""
    surrogate_key: str = ""


# ── Component AST ────────────────────────────────────────

class ComponentAST(BaseModel):
    unique_name: str                    # "tDBInput_1"
    component_type: str                 # "tMysqlInput"
    behavior: ComponentBehavior = ComponentBehavior.AI_REQUIRED
    parameters: dict[str, str] = Field(default_factory=dict)

    # Source/Sink info (populated for INPUT/OUTPUT components)
    source_info: Optional[SourceInfo] = None

    # Transform-specific (populated for tMap)
    tmap_data: Optional[TMapData] = None

    # Filter/Aggregate/Unique/Sort (populated for respective types)
    filter_conditions: list[FilterCondition] = Field(default_factory=list)
    aggregate_config: Optional[AggregateConfig] = None
    unique_config: Optional[UniqueConfig] = None
    sort_config: Optional[SortConfig] = None
    scd_config: Optional[SCDConfig] = None

    # Custom code
    java_code: str = ""

    # Schema per connector
    schemas: dict[str, list[ColumnSchema]] = Field(default_factory=dict)

    # Orchestration (tRunJob)
    child_job_name: str = ""

    # tSetGlobalVar entries
    global_var_entries: dict[str, str] = Field(default_factory=dict)

    # Confidence
    confidence: float = 1.0  # 1.0 = fully deterministic, 0.0 = unknown

    # Raw XML preserved for LLM fallback
    raw_xml: str = ""


# ── Connection (Flow Edge) ───────────────────────────────

class FlowConnection(BaseModel):
    source: str                # "tDBInput_1"
    target: str                # "tMap_1"
    trigger_type: TriggerType = TriggerType.FLOW
    flow_name: str = ""        # "row1", "OnSubjobOk"
    connector_name: str = ""   # raw connectorName attribute
    condition: str = ""        # RUN_IF condition expression


# ── Job AST ──────────────────────────────────────────────

class JobAST(BaseModel):
    name: str
    label: str = ""
    version: str = ""
    file_path: str = ""        # relative path to .item
    screenshot_path: str = ""  # relative path to .screenshot
    job_type: JobType = JobType.STANDARD
    default_context: str = "Default"

    components: dict[str, ComponentAST] = Field(default_factory=dict)
    connections: list[FlowConnection] = Field(default_factory=list)
    contexts: dict[str, dict[str, str]] = Field(default_factory=dict)
    execution_order: list[str] = Field(default_factory=list)

    # Orchestration
    child_jobs: list[str] = Field(default_factory=list)
    has_parallelize: bool = False

    # Flow graph: connection_label → source_component_unique_name
    # Built from <connection> elements. Used by sql_generator for ref resolution.
    flow_name_map: dict[str, str] = Field(default_factory=dict)

    # Data lineage (populated by data_lineage.py)
    reads_tables: list[str] = Field(default_factory=list)   # tables this job reads from
    writes_tables: list[str] = Field(default_factory=list)  # tables this job writes to

    # Confidence scoring
    deterministic_pct: float = 0.0
    needs_llm: list[str] = Field(default_factory=list)
    flagged: list[str] = Field(default_factory=list)


# ── Project AST (Top-Level) ─────────────────────────────

class ProjectAST(BaseModel):
    project_name: str = ""
    scan_timestamp: str = ""
    input_path: str = ""

    jobs: dict[str, JobAST] = Field(default_factory=dict)
    joblets: dict[str, JobAST] = Field(default_factory=dict)
    contexts: dict[str, dict[str, str]] = Field(default_factory=dict)

    # Inter-job DAG
    job_dag_edges: list[dict] = Field(default_factory=list)
    build_order: list[str] = Field(default_factory=list)
    roots: list[str] = Field(default_factory=list)
    parallel_groups: list[list[str]] = Field(default_factory=list)
    connected_jobs: list[str] = Field(default_factory=list)   # jobs IN the DAG (have edges)
    orphan_jobs: list[str] = Field(default_factory=list)       # truly dead jobs (no orch + no data deps)

    # Data lineage (populated by data_lineage.py)
    data_dependent_jobs: list[str] = Field(default_factory=list)   # orch-orphans needed for data
    dead_jobs: list[str] = Field(default_factory=list)             # truly dead — skip migration
    data_lineage_edges: list[dict] = Field(default_factory=list)   # [{writer, reader, table}]

    # Source catalog
    source_catalog: dict[str, SourceInfo] = Field(default_factory=dict)

    # Stats
    total_components: int = 0
    components_by_behavior: dict[str, int] = Field(default_factory=dict)
