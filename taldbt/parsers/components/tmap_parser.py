"""
Deep tMap parser: extracts all join keys, expressions, filters, and output mappings.
This is where 80% of Talend business logic lives.
"""
from lxml import etree
from taldbt.models.ast_models import (
    ComponentAST, TMapData, TMapInput, TMapOutput, TMapExpression,
    TMapJoinKey, TMapOutputFilter, TMapVarEntry, ExpressionStrategy,
)


def _local_tag(elem) -> str:
    tag = elem.tag
    return tag.split("}")[1] if "}" in tag else tag


def _classify_expression(expr: str) -> ExpressionStrategy:
    """Determine if a tMap expression can be translated deterministically."""
    if not expr or expr.strip() == "":
        return ExpressionStrategy.DETERMINISTIC

    e = expr.strip()

    # Simple column reference: "row1.column_name"
    if "." in e and not "(" in e and not " " in e:
        return ExpressionStrategy.DETERMINISTIC

    # Literal values
    if e.startswith('"') or e.startswith("'") or e.replace(".", "").replace("-", "").isdigit():
        return ExpressionStrategy.DETERMINISTIC

    # Null checks
    if "== null" in e or "!= null" in e or "Relational.ISNULL" in e:
        return ExpressionStrategy.KNOWLEDGE_BASE

    # Arithmetic
    if all(c in "0123456789.+-*/() " or c.isalpha() or c == "_" for c in e.replace("row", "").replace(".", "")):
        if any(op in e for op in [" + ", " - ", " * ", " / "]):
            return ExpressionStrategy.DETERMINISTIC

    # Known Talend routines
    known_prefixes = [
        "StringHandling.", "TalendDate.", "Numeric.", "Relational.",
        "Mathematical.", "DataOperation.", "TalendString.",
    ]
    if any(e.startswith(p) or p in e for p in known_prefixes):
        return ExpressionStrategy.KNOWLEDGE_BASE

    # context variable references
    if "context." in e or "globalMap" in e:
        return ExpressionStrategy.KNOWLEDGE_BASE

    # Ternary operator (common Java pattern)
    if "?" in e and ":" in e:
        # Simple ternaries can be translated to CASE WHEN
        return ExpressionStrategy.KNOWLEDGE_BASE

    # String concatenation
    if ' + "' in e or '" + ' in e:
        return ExpressionStrategy.KNOWLEDGE_BASE

    # Java method calls → needs LLM
    if "." in e and "(" in e:
        return ExpressionStrategy.LLM_REQUIRED

    return ExpressionStrategy.DETERMINISTIC


def parse_tmap(comp: ComponentAST, node_elem) -> None:
    """Parse tMap nodeData and populate comp.tmap_data."""
    tmap = TMapData()

    # Find the nodeData child
    node_data = None
    for child in node_elem:
        tag = _local_tag(child)
        if "nodeData" in tag:
            node_data = child
            break

    if node_data is None:
        # Fallback: try TalendMapper namespace
        for child in node_elem.iter():
            tag = _local_tag(child)
            if tag in ("nodeData", "NodeData"):
                node_data = child
                break

    if node_data is None:
        comp.confidence = 0.5
        comp.tmap_data = tmap
        return

    is_first_input = True

    for child in node_data:
        tag = _local_tag(child)

        # ── Input Tables ─────────────────────────────
        if "inputTable" in tag:
            inp = TMapInput(
                name=child.get("name", ""),
                matching_mode=child.get("matchingMode", "ALL_ROWS"),
                is_main_input=is_first_input,
            )
            is_first_input = False

            # Join type
            jt = child.get("joinType", "") or child.get("type", "")
            if "INNER" in jt.upper():
                inp.join_type = "INNER"
            elif "LEFT" in jt.upper():
                inp.join_type = "LEFT_OUTER"
            elif "CROSS" in jt.upper():
                inp.join_type = "CROSS"
            else:
                inp.join_type = "INNER" if not inp.is_main_input else "MAIN"

            # Join keys from mapperTableEntries with expressions
            for entry in child:
                if "mapperTableEntries" in _local_tag(entry):
                    expr = entry.get("expression", "")
                    col_name = entry.get("name", "")
                    if expr and not inp.is_main_input:
                        # This is a join key
                        inp.join_keys.append(TMapJoinKey(name=col_name, expression=expr))

            tmap.inputs.append(inp)

        # ── Output Tables ────────────────────────────
        elif "outputTable" in tag:
            out = TMapOutput(
                name=child.get("name", ""),
                is_reject=child.get("reject", "false").lower() == "true",
            )

            # Filter expression
            filter_expr = child.get("expressionFilter", "")
            if filter_expr:
                strategy = _classify_expression(filter_expr)
                out.filter = TMapOutputFilter(expression=filter_expr, strategy=strategy)

            # Output columns
            for entry in child:
                if "mapperTableEntries" in _local_tag(entry):
                    col_name = entry.get("name", "")
                    expr = entry.get("expression", "")
                    expr_type = entry.get("type", "")

                    strategy = _classify_expression(expr)
                    out.columns.append(TMapExpression(
                        column_name=col_name,
                        expression=expr or "",
                        expression_type=expr_type,
                        strategy=strategy,
                    ))

            tmap.outputs.append(out)

        # ── Var Table ────────────────────────────────
        elif "varTable" in tag:
            for entry in child:
                if "mapperTableEntries" in _local_tag(entry):
                    tmap.var_table.append(TMapVarEntry(
                        name=entry.get("name", ""),
                        expression=entry.get("expression", ""),
                        type=entry.get("type", ""),
                    ))

    # Set confidence based on expression strategies
    total_exprs = sum(len(o.columns) for o in tmap.outputs)
    llm_exprs = sum(
        1 for o in tmap.outputs
        for c in o.columns
        if c.strategy == ExpressionStrategy.LLM_REQUIRED
    )
    if total_exprs > 0:
        comp.confidence = 1.0 - (llm_exprs / total_exprs)
    else:
        comp.confidence = 0.8

    comp.tmap_data = tmap
