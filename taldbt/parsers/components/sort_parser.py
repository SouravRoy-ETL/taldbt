"""
tSortRow parser: extracts sort columns and directions from TABLE-type CRITERIA.

Talend stores sort criteria as a TABLE parameter named "CRITERIA":
  - SCHEMA_COLUMN: column name
  - ORDER: "asc" or "desc"
  - TYPE: data type (for comparison, not needed in SQL)
"""
from lxml import etree
from taldbt.models.ast_models import ComponentAST, SortConfig


def _local_tag(elem) -> str:
    tag = elem.tag
    return tag.split("}")[1] if "}" in tag else tag


def parse_sort(comp: ComponentAST, node_elem=None) -> None:
    """Extract sort criteria from tSortRow."""
    config = SortConfig()

    root = node_elem
    if root is None and comp.raw_xml:
        try:
            root = etree.fromstring(comp.raw_xml)
        except Exception:
            root = None

    if root is None:
        comp.sort_config = config
        comp.confidence = 0.5
        return

    for param in root.iter():
        tag = _local_tag(param)
        if tag != "elementParameter":
            continue

        param_name = param.get("name", "")
        if param_name != "CRITERIA":
            continue

        values_by_ref: dict[str, list[str]] = {}
        for ev in param:
            if _local_tag(ev) == "elementValue":
                ref = ev.get("elementRef", "")
                val = ev.get("value", "")
                values_by_ref.setdefault(ref, []).append(val)

        columns = values_by_ref.get("SCHEMA_COLUMN", [])
        orders = values_by_ref.get("ORDER", [])

        for i in range(len(columns)):
            col = columns[i].strip().strip('"') if i < len(columns) else ""
            order = orders[i].strip().strip('"').upper() if i < len(orders) else "ASC"
            if col:
                config.columns.append(col)
                config.orders.append("DESC" if "DESC" in order else "ASC")

    comp.sort_config = config
    comp.confidence = 0.95 if config.columns else 0.5
