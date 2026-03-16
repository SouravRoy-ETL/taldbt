"""
Behavior-based Component Classifier.
Maps 800+ Talend component names → ~15 behavior buckets using suffix matching.

Classification hierarchy:
  1. Exact match (60+ known components)
  2. Suffix match (longest suffix first, handles all vendor variants)
  3. Fallback: AI_REQUIRED
"""
from taldbt.models.ast_models import ComponentBehavior


# ═══════════════════════════════════════════════════════════
# Exact matches for well-known components
# ═══════════════════════════════════════════════════════════

_EXACT_MAP: dict[str, ComponentBehavior] = {
    # ── Transformers ──────────────────────────────────
    "tMap":             ComponentBehavior.TRANSFORMER,
    "tXMLMap":          ComponentBehavior.TRANSFORMER,
    "tELTMap":          ComponentBehavior.TRANSFORMER,
    "tFilterRow":       ComponentBehavior.FILTER,
    "tFilterColumns":   ComponentBehavior.PROJECTION,
    "tAggregateRow":    ComponentBehavior.AGGREGATE,
    "tUniqRow":         ComponentBehavior.DEDUP,
    "tSortRow":         ComponentBehavior.SORT,
    "tJoin":            ComponentBehavior.JOINER,
    "tUnite":           ComponentBehavior.UNION,
    "tNormalize":       ComponentBehavior.UNPIVOT,
    "tDenormalize":     ComponentBehavior.PIVOT,
    "tReplace":         ComponentBehavior.TRANSFORM_SIMPLE,
    "tConvertType":     ComponentBehavior.TRANSFORM_SIMPLE,
    "tPivot":           ComponentBehavior.PIVOT,
    "tUnpivot":         ComponentBehavior.UNPIVOT,
    "tReplicate":       ComponentBehavior.TRANSFORM_SIMPLE,
    "tSampleRow":       ComponentBehavior.FILTER,
    "tExtractXMLField": ComponentBehavior.TRANSFORMER,
    "tExtractJSONFields": ComponentBehavior.TRANSFORMER,
    "tSchemaComplianceCheck": ComponentBehavior.FILTER,

    # ── Custom code ───────────────────────────────────
    "tJavaRow":     ComponentBehavior.CUSTOM_CODE,
    "tJava":        ComponentBehavior.CUSTOM_CODE,
    "tJavaFlex":    ComponentBehavior.CUSTOM_CODE,
    "tGroovy":      ComponentBehavior.CUSTOM_CODE,
    "tGroovyFile":  ComponentBehavior.CUSTOM_CODE,
    "tSetDynamicSchema": ComponentBehavior.IGNORE,

    # ── Orchestration ─────────────────────────────────
    "tRunJob":          ComponentBehavior.ORCHESTRATION,
    "tParallelize":     ComponentBehavior.ORCHESTRATION,
    "tLoop":            ComponentBehavior.ORCHESTRATION,
    "tForeach":         ComponentBehavior.ORCHESTRATION,
    "tInfiniteLoop":    ComponentBehavior.ORCHESTRATION,
    "tFlowToIterate":   ComponentBehavior.ITERATOR,
    "tIterateToFlow":   ComponentBehavior.ITERATOR,
    "tFileList":        ComponentBehavior.ORCHESTRATION,
    "tWaitForFile":     ComponentBehavior.ORCHESTRATION,
    "tWaitForSocket":   ComponentBehavior.ORCHESTRATION,
    "tWaitForSqlData":  ComponentBehavior.ORCHESTRATION,
    "tSleep":           ComponentBehavior.ORCHESTRATION,
    "tDie":             ComponentBehavior.ORCHESTRATION,
    "tWarn":            ComponentBehavior.OBSERVABILITY,
    "tBarrier":         ComponentBehavior.ORCHESTRATION,

    # ── State ─────────────────────────────────────────
    "tSetGlobalVar":    ComponentBehavior.STATE_MANAGER,
    "tContextLoad":     ComponentBehavior.STATE_MANAGER,
    "tContextDump":     ComponentBehavior.STATE_MANAGER,
    "tBufferInput":     ComponentBehavior.STATE_MANAGER,
    "tBufferOutput":    ComponentBehavior.STATE_MANAGER,
    "tHashInput":       ComponentBehavior.STATE_MANAGER,
    "tHashOutput":      ComponentBehavior.STATE_MANAGER,

    # ── Observability / Logging ───────────────────────
    "tLogCatcher":          ComponentBehavior.OBSERVABILITY,
    "tStatCatcher":         ComponentBehavior.OBSERVABILITY,
    "tAssert":              ComponentBehavior.OBSERVABILITY,
    "tAssertCatcher":       ComponentBehavior.OBSERVABILITY,
    "tFlowMeter":           ComponentBehavior.OBSERVABILITY,
    "tFlowMeterCatcher":    ComponentBehavior.OBSERVABILITY,
    "tChronometerStart":    ComponentBehavior.OBSERVABILITY,
    "tChronometerStop":     ComponentBehavior.OBSERVABILITY,

    # ── Ignore (no data impact) ───────────────────────
    "tPrejob":          ComponentBehavior.IGNORE,
    "tPostjob":         ComponentBehavior.IGNORE,
    "tLibraryLoad":     ComponentBehavior.IGNORE,
    "tLogRow":          ComponentBehavior.IGNORE,
    "tMsgBox":          ComponentBehavior.IGNORE,
    "tCreateTable":     ComponentBehavior.DDL,
    "tDropTable":       ComponentBehavior.DDL,
    "tTruncate":        ComponentBehavior.DDL,

    # ── Generic/virtual input components ──────────────
    "tFixedFlowInput":  ComponentBehavior.DATA_SOURCE,
    "tRowGenerator":    ComponentBehavior.DATA_SOURCE,
    "tELTInput":        ComponentBehavior.DATA_SOURCE,
    "tDBInput":         ComponentBehavior.DATA_SOURCE,
    "tDBOutput":        ComponentBehavior.DATA_SINK,
    "tDBRow":           ComponentBehavior.SQL_EXEC,
    "tDBSP":            ComponentBehavior.SQL_EXEC,

    # ── File components (not caught by suffix) ───────
    "tFileInputDelimited":  ComponentBehavior.DATA_SOURCE,
    "tFileInputCSV":        ComponentBehavior.DATA_SOURCE,
    "tFileInputExcel":      ComponentBehavior.DATA_SOURCE,
    "tFileInputJSON":       ComponentBehavior.DATA_SOURCE,
    "tFileInputXML":        ComponentBehavior.DATA_SOURCE,
    "tFileInputParquet":    ComponentBehavior.DATA_SOURCE,
    "tFileInputFullRow":    ComponentBehavior.DATA_SOURCE,
    "tFileInputPositional": ComponentBehavior.DATA_SOURCE,
    "tFileInputRegex":      ComponentBehavior.DATA_SOURCE,
    "tFileInputLDIF":       ComponentBehavior.DATA_SOURCE,
    "tFileOutputDelimited": ComponentBehavior.DATA_SINK,
    "tFileOutputCSV":       ComponentBehavior.DATA_SINK,
    "tFileOutputExcel":     ComponentBehavior.DATA_SINK,
    "tFileOutputJSON":      ComponentBehavior.DATA_SINK,
    "tFileOutputXML":       ComponentBehavior.DATA_SINK,
    "tFileOutputParquet":   ComponentBehavior.DATA_SINK,

    # ── SCD ───────────────────────────────────────────
    "tDBSCD":           ComponentBehavior.SCD_HANDLER,
    "tMysqlSCD":        ComponentBehavior.SCD_HANDLER,
    "tOracleSCD":       ComponentBehavior.SCD_HANDLER,
    "tMSSqlSCD":        ComponentBehavior.SCD_HANDLER,
    "tPostgresSCD":     ComponentBehavior.SCD_HANDLER,
    "tSnowflakeSCD":    ComponentBehavior.SCD_HANDLER,
    "tELTSCD":          ComponentBehavior.SCD_HANDLER,
}


# ═══════════════════════════════════════════════════════════
# Suffix matches (longest first) for vendor variants
# tMysqlInput, tOracleInput, tSnowflakeInput → all DATA_SOURCE
# ═══════════════════════════════════════════════════════════

_SUFFIX_MAP: list[tuple[str, ComponentBehavior]] = sorted([
    # Data flow
    ("Input", ComponentBehavior.DATA_SOURCE),
    ("Output", ComponentBehavior.DATA_SINK),
    ("BulkExec", ComponentBehavior.DATA_SINK),
    ("OutputBulkExec", ComponentBehavior.DATA_SINK),
    ("OutputBulk", ComponentBehavior.DATA_SINK),
    ("Unload", ComponentBehavior.DATA_SINK),
    # Connection lifecycle
    ("Connection", ComponentBehavior.IGNORE),
    ("Close", ComponentBehavior.IGNORE),
    ("Commit", ComponentBehavior.IGNORE),
    ("Rollback", ComponentBehavior.IGNORE),
    ("LastInsertId", ComponentBehavior.IGNORE),
    # SQL execution
    ("Row", ComponentBehavior.SQL_EXEC),
    ("SP", ComponentBehavior.SQL_EXEC),
    # Change data capture
    ("CDC", ComponentBehavior.DATA_SOURCE),
    ("CDCOutput", ComponentBehavior.DATA_SINK),
    # SCD
    ("SCD", ComponentBehavior.SCD_HANDLER),
    ("SCDELT", ComponentBehavior.SCD_HANDLER),
    # File operations
    ("Get", ComponentBehavior.FILE_FETCH),
    ("Put", ComponentBehavior.FILE_PUSH),
    # Side effects
    ("Delete", ComponentBehavior.SIDE_EFFECT),
    ("Copy", ComponentBehavior.SIDE_EFFECT),
    ("Create", ComponentBehavior.SIDE_EFFECT),
    ("Exist", ComponentBehavior.SIDE_EFFECT),
    ("Manage", ComponentBehavior.SIDE_EFFECT),
    ("Purge", ComponentBehavior.SIDE_EFFECT),
    ("Rename", ComponentBehavior.SIDE_EFFECT),
    ("Deploy", ComponentBehavior.SIDE_EFFECT),
    ("Resize", ComponentBehavior.SIDE_EFFECT),
    # Iterator
    ("List", ComponentBehavior.ITERATOR),
], key=lambda x: len(x[0]), reverse=True)  # longest suffix first


def classify(component_name: str) -> ComponentBehavior:
    """Classify any Talend component into a behavior bucket.

    Classification order:
      1. Exact match in _EXACT_MAP
      2. Suffix match (longest first) in _SUFFIX_MAP
      3. Fallback: AI_REQUIRED
    """
    # 1. Exact match
    if component_name in _EXACT_MAP:
        return _EXACT_MAP[component_name]

    # 2. Suffix match (handles all 800+ vendor variants)
    for suffix, behavior in _SUFFIX_MAP:
        if component_name.endswith(suffix):
            return behavior

    # 3. Unknown → needs AI
    return ComponentBehavior.AI_REQUIRED
