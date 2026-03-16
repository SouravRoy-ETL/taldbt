"""tAggregateRow parser: extracts GROUP BY keys and aggregate operations."""
from lxml import etree
from taldbt.models.ast_models import ComponentAST, AggregateConfig, AggregateOperation


def _local_tag(elem) -> str:
    tag = elem.tag
    return tag.split("}")[1] if "}" in tag else tag


# Talend function names → SQL aggregate functions
_AGG_FUNC_MAP = {
    "sum": "SUM",
    "count": "COUNT",
    "min": "MIN",
    "max": "MAX",
    "avg": "AVG",
    "first": "FIRST_VALUE",
    "last": "LAST_VALUE",
    "list": "STRING_AGG",
    "count_distinct": "COUNT(DISTINCT",
    "standard_deviation": "STDDEV",
    "median": "MEDIAN",
}


def parse_aggregate(comp: ComponentAST, node_elem=None) -> None:
    """Extract aggregation config from tAggregateRow.

    tAggregateRow stores two TABLE-type parameters:
      - GROUP_BY: rows of {SCHEMA_COLUMN} defining the GROUP BY keys
      - OPERATIONS: rows of {INPUT_COLUMN, FUNCTION, OUTPUT_COLUMN}

    These are stored as nested <elementValue> children inside
    <elementParameter name="GROUP_BY"> and <elementParameter name="OPERATIONS">.
    """
    config = AggregateConfig()

    # Determine source: prefer live element, fallback to stored raw_xml
    root = node_elem
    if root is None and comp.raw_xml:
        root = etree.fromstring(comp.raw_xml)
    if root is None:
        comp.aggregate_config = config
        comp.confidence = 0.5
        return

    # ── Extract GROUP_BY columns ────────────────────────
    for param in root.iter():
        tag = _local_tag(param)
        if tag != "elementParameter":
            continue

        param_name = param.get("name", "")

        if param_name == "GROUP_BY":
            # TABLE param: children are <elementValue> with elementRef/value pairs
            # Talend stores them as pairs: elementRef="SCHEMA_COLUMN", value="ColName"
            for ev in param:
                if _local_tag(ev) == "elementValue":
                    ref = ev.get("elementRef", "")
                    val = ev.get("value", "")
                    if ref == "SCHEMA_COLUMN" and val:
                        config.group_by.append(val)

        elif param_name == "OPERATIONS":
            # TABLE param: children come in triples per operation row
            # elementRef="INPUT_COLUMN", elementRef="FUNCTION", elementRef="OUTPUT_COLUMN"
            # They arrive as sequential <elementValue> tags — every 3 form one operation
            values_by_ref: dict[str, list[str]] = {}
            for ev in param:
                if _local_tag(ev) == "elementValue":
                    ref = ev.get("elementRef", "")
                    val = ev.get("value", "")
                    values_by_ref.setdefault(ref, []).append(val)

            input_cols = values_by_ref.get("INPUT_COLUMN", [])
            functions = values_by_ref.get("FUNCTION", [])
            output_cols = values_by_ref.get("OUTPUT_COLUMN", [])

            # Zip them together — they should be parallel lists of equal length
            for i in range(len(functions)):
                func_raw = functions[i].strip().strip('"').lower() if i < len(functions) else ""
                in_col = input_cols[i].strip().strip('"') if i < len(input_cols) else ""
                out_col = output_cols[i].strip().strip('"') if i < len(output_cols) else in_col

                sql_func = _AGG_FUNC_MAP.get(func_raw, func_raw.upper())

                config.operations.append(AggregateOperation(
                    output_column=out_col,
                    function=sql_func,
                    input_column=in_col,
                ))

    comp.aggregate_config = config
    comp.confidence = 0.95 if (config.group_by or config.operations) else 0.5
