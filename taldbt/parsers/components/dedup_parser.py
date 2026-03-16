"""
tUniqRow parser: extracts dedup key columns from TABLE-type UNIQUE_KEY.

Talend stores dedup keys as a TABLE parameter named "UNIQUE_KEY" or "KEY_ATTRIBUTE":
  - ATTRIBUTE: column name
  - CASE_SENSITIVE: "true" or "false"
  - KEY_TYPE: "UNIQUE_KEY" (for the key column list)
"""
from lxml import etree
from taldbt.models.ast_models import ComponentAST, UniqueConfig


def _local_tag(elem) -> str:
    tag = elem.tag
    return tag.split("}")[1] if "}" in tag else tag


def parse_dedup(comp: ComponentAST, node_elem=None) -> None:
    """Extract dedup key columns from tUniqRow."""
    config = UniqueConfig()

    root = node_elem
    if root is None and comp.raw_xml:
        try:
            root = etree.fromstring(comp.raw_xml)
        except Exception:
            root = None

    if root is None:
        comp.unique_config = config
        comp.confidence = 0.5
        return

    for param in root.iter():
        tag = _local_tag(param)
        if tag != "elementParameter":
            continue

        param_name = param.get("name", "")

        if param_name == "UNIQUE_KEY":
            values_by_ref: dict[str, list[str]] = {}
            for ev in param:
                if _local_tag(ev) == "elementValue":
                    ref = ev.get("elementRef", "")
                    val = ev.get("value", "")
                    values_by_ref.setdefault(ref, []).append(val)

            attributes = values_by_ref.get("ATTRIBUTE", [])
            key_types = values_by_ref.get("KEY_TYPE", [])

            for i in range(len(attributes)):
                col = attributes[i].strip().strip('"') if i < len(attributes) else ""
                ktype = key_types[i].strip().strip('"') if i < len(key_types) else "UNIQUE_KEY"
                if col and ktype == "UNIQUE_KEY":
                    config.key_columns.append(col)

        elif param_name == "CASE_SENSITIVE":
            config.case_sensitive = param.get("value", "true").lower() == "true"

    # Fallback: if no KEY_TYPE filtering found, treat all attributes as keys
    if not config.key_columns:
        for param in root.iter():
            tag = _local_tag(param)
            if tag != "elementParameter":
                continue
            if param.get("name") == "UNIQUE_KEY":
                for ev in param:
                    if _local_tag(ev) == "elementValue":
                        ref = ev.get("elementRef", "")
                        val = ev.get("value", "")
                        if ref in ("ATTRIBUTE", "SCHEMA_COLUMN") and val:
                            col = val.strip().strip('"')
                            if col and col not in config.key_columns:
                                config.key_columns.append(col)

    comp.unique_config = config
    comp.confidence = 0.95 if config.key_columns else 0.5
