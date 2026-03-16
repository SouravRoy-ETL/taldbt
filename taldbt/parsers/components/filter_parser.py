"""
tFilterRow parser: extracts filter conditions from TABLE-type CONDITIONS parameter.

Talend stores filter conditions as sequential <elementValue> children inside
<elementParameter name="CONDITIONS">. Each condition row has:
  - INPUT_COLUMN: the column being tested
  - FUNCTION: comparison operator (==, !=, <, >, <=, >=, MATCHES, CONTAINS, etc.)
  - OPERATOR: (not always present — sometimes FUNCTION holds the operator)
  - RVALUE: the comparison value
  - JOIN: logical connector (AND / OR)
"""
from lxml import etree
from taldbt.models.ast_models import ComponentAST, FilterCondition


def _local_tag(elem) -> str:
    tag = elem.tag
    return tag.split("}")[1] if "}" in tag else tag


def parse_filter(comp: ComponentAST, node_elem=None) -> None:
    """Extract filter conditions from tFilterRow XML or parameters.

    TABLE-type parameter "CONDITIONS" stores parallel lists:
      elementRef="INPUT_COLUMN" → column name
      elementRef="FUNCTION"     → operator string
      elementRef="OPERATOR"     → sometimes the real operator
      elementRef="RVALUE"       → comparison value
      elementRef="JOIN"         → AND / OR connector

    They arrive as sequential <elementValue> children.
    """
    conditions: list[FilterCondition] = []

    # Determine XML source: prefer live element, fallback to raw_xml
    root = node_elem
    if root is None and comp.raw_xml:
        try:
            root = etree.fromstring(comp.raw_xml)
        except Exception:
            root = None

    if root is None:
        comp.filter_conditions = conditions
        comp.confidence = 0.5
        return

    # Parse TABLE-type parameter named "CONDITIONS" or "CONDITION"
    for param in root.iter():
        tag = _local_tag(param)
        if tag != "elementParameter":
            continue

        param_name = param.get("name", "")
        if param_name not in ("CONDITIONS", "CONDITION"):
            continue

        # Collect parallel lists from elementValue children
        values_by_ref: dict[str, list[str]] = {}
        for ev in param:
            if _local_tag(ev) == "elementValue":
                ref = ev.get("elementRef", "")
                val = ev.get("value", "")
                values_by_ref.setdefault(ref, []).append(val)

        input_cols = values_by_ref.get("INPUT_COLUMN", [])
        functions = values_by_ref.get("FUNCTION", [])
        operators = values_by_ref.get("OPERATOR", [])
        rvalues = values_by_ref.get("RVALUE", [])
        joins = values_by_ref.get("JOIN", [])

        # Build conditions from parallel lists
        count = max(len(input_cols), len(functions))
        for i in range(count):
            col = input_cols[i].strip().strip('"') if i < len(input_cols) else ""
            func = functions[i].strip().strip('"') if i < len(functions) else "=="
            op = operators[i].strip().strip('"') if i < len(operators) else ""
            val = rvalues[i].strip().strip('"') if i < len(rvalues) else ""
            join = joins[i].strip().strip('"') if i < len(joins) else "AND"

            if not col:
                continue

            # Talend uses "FUNCTION" as the operator name. Normalize.
            effective_op = op if op else func

            conditions.append(FilterCondition(
                input_column=col,
                function=effective_op,
                value=val,
                logical_op=join.upper() if join else "AND",
            ))

    # Also check for LOGICAL_OP parameter (top-level AND/OR selector)
    for param in root.iter():
        tag = _local_tag(param)
        if tag == "elementParameter" and param.get("name") == "LOGICAL_OP":
            global_op = param.get("value", "AND").strip('"').upper()
            # Apply to all conditions
            for cond in conditions:
                cond.logical_op = global_op

    comp.filter_conditions = conditions
    comp.confidence = 0.95 if conditions else 0.5
