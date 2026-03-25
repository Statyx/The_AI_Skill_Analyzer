"""TMDL parser — extract tables, columns, measures, relationships from TMDL definition parts."""

import base64


def parse_tmdl_tables(parts):
    """Parse TMDL definition parts into (tables_dict, relationships_list)."""
    tables = {}
    relationships = []

    for part in parts:
        path = part.get("path", "")
        payload = part.get("payload", "")
        if part.get("payloadType") == "InlineBase64" and payload:
            try:
                payload = base64.b64decode(payload).decode("utf-8")
            except Exception:
                continue
        if not isinstance(payload, str):
            continue

        if path.startswith("definition/tables/") and path.endswith(".tmdl"):
            table_name = None
            table_desc = ""
            columns = []
            measures = []
            pending_desc = []

            for line in payload.split("\n"):
                stripped = line.strip()
                if stripped.startswith("///"):
                    pending_desc.append(stripped[3:].strip())
                    continue
                if stripped.startswith("table "):
                    table_name = stripped.split("table ", 1)[1].strip().strip("'")
                    table_desc = " ".join(pending_desc) if pending_desc else ""
                    pending_desc = []
                elif stripped.startswith("measure "):
                    rest = stripped.split("measure ", 1)[1].strip()
                    meas_name = rest.split(" = ")[0].strip().strip("'") if " = " in rest else rest.strip().strip("'")
                    desc = " ".join(pending_desc) if pending_desc else ""
                    pending_desc = []
                    measures.append({"name": meas_name, "description": desc,
                                     "expression": "", "format_string": ""})
                elif stripped.startswith("column "):
                    col_name = stripped.split("column ", 1)[1].strip().strip("'")
                    desc = " ".join(pending_desc) if pending_desc else ""
                    pending_desc = []
                    columns.append({"name": col_name, "description": desc,
                                    "data_type": "", "is_hidden": False})
                elif stripped.startswith("dataType:") and columns:
                    columns[-1]["data_type"] = stripped.split(":", 1)[1].strip()
                elif stripped == "isHidden" and columns:
                    columns[-1]["is_hidden"] = True
                elif stripped.startswith("formatString:") and measures:
                    measures[-1]["format_string"] = stripped.split(":", 1)[1].strip().strip('"')
                else:
                    if not stripped.startswith("lineageTag:") and not stripped.startswith("sourceLineageTag:"):
                        pending_desc = []

            if table_name:
                tables[table_name] = {"name": table_name, "description": table_desc,
                                      "columns": columns, "measures": measures}

        elif "relationships" in path and path.endswith(".tmdl"):
            for line in payload.split("\n"):
                stripped = line.strip()
                if stripped.startswith("relationship "):
                    relationships.append(stripped)

    return tables, relationships


def build_schema(parts, cfg):
    """Convert TMDL definition parts into a portal-equivalent schema JSON."""
    tmap, rels = parse_tmdl_tables(parts)
    elements = []
    for tinfo in tmap.values():
        children = []
        for col in tinfo["columns"]:
            children.append({
                "display_name": col["name"], "type": "semantic_model.column",
                "description": col["description"], "data_type": col.get("data_type", ""),
                "is_hidden": col.get("is_hidden", False),
            })
        for meas in tinfo["measures"]:
            children.append({
                "display_name": meas["name"], "type": "semantic_model.measure",
                "description": meas["description"], "expression": meas.get("expression", ""),
                "format_string": meas.get("format_string", ""),
            })
        elements.append({
            "display_name": tinfo["name"], "type": "semantic_model.table",
            "description": tinfo["description"], "is_selected": True, "children": children,
        })

    n_tables = len(tmap)
    n_cols = sum(len(t["columns"]) for t in tmap.values())
    n_measures = sum(len(t["measures"]) for t in tmap.values())
    desc_t = sum(1 for t in tmap.values() if t["description"])
    desc_c = sum(1 for t in tmap.values() for c in t["columns"] if c["description"])
    desc_m = sum(1 for t in tmap.values() for m in t["measures"] if m["description"])

    return {
        "dataSourceInfo": {
            "type": "semantic_model",
            "semantic_model_id": cfg["semantic_model_id"],
            "semantic_model_name": cfg["semantic_model_name"],
            "semantic_model_workspace_id": cfg["workspace_id"],
        },
        "elements": elements,
        "relationships": rels,
        "stats": {
            "tables": n_tables, "columns": n_cols, "measures": n_measures,
            "relationships": len(rels),
            "description_coverage": {
                "tables": f"{desc_t}/{n_tables}",
                "columns": f"{desc_c}/{n_cols}",
                "measures": f"{desc_m}/{n_measures}",
            },
        },
    }


def empty_schema(cfg):
    return {
        "dataSourceInfo": {
            "type": "semantic_model",
            "semantic_model_id": cfg["semantic_model_id"],
            "semantic_model_name": cfg["semantic_model_name"],
            "semantic_model_workspace_id": cfg["workspace_id"],
        },
        "elements": [], "relationships": [],
        "stats": {
            "tables": 0, "columns": 0, "measures": 0, "relationships": 0,
            "description_coverage": {"tables": "0/0", "columns": "0/0", "measures": "0/0"},
        },
    }
