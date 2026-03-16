"""
Test Data Generator v4: Faker-powered intelligent data generation.

Uses Python Faker to generate realistic, contextual test data based on
column names and types. No more 'test_colname_1' garbage.

Architecture:
  1. Faker profiles per column name pattern
  2. FK-aware: scans SQL JOINs, ensures matching values
  3. Column reconciliation from generated SQL
  4. Source tables created directly in dbt source schemas
"""
from __future__ import annotations
import random
import re
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

try:
    from faker import Faker
    _fake = Faker()
    Faker.seed(42)
    HAS_FAKER = True
except ImportError:
    _fake = None
    HAS_FAKER = False

from taldbt.models.ast_models import ProjectAST, ColumnSchema


# =====================================================================
# Faker-Powered Value Generator
# =====================================================================

def _is_joinable_col(col) -> bool:
    name = col.name.lower()
    return col.is_key or any(kw in name for kw in (
        'id', 'key', 'sk', 'fk', 'entityid', 'orderid',
        'code', 'number', 'num', 'no', 'ref',
    ))


def _gen_value(col, row_idx, used_ids=None):
    """Generate a realistic test value using Faker when available."""
    st = col.sql_type.upper().split("(")[0].strip() if col.sql_type else "VARCHAR"
    name = col.name.lower()

    # Join keys: sequential, deterministic
    if _is_joinable_col(col):
        if st in ("DATE", "TIMESTAMP", "DATETIME", "DATETIME2"):
            d = datetime(2020, 1, 1) + timedelta(days=row_idx)
            return f"DATE '{d.strftime('%Y-%m-%d')}'" if st == "DATE" else f"TIMESTAMP '{d.strftime('%Y-%m-%d %H:%M:%S')}'"
        if st in ("VARCHAR", "CHAR", "NVARCHAR", "TEXT", ""):
            return f"'{row_idx + 1}'"
        if st in ("BOOLEAN", "BIT"):
            return random.choice(["TRUE", "FALSE"])
        return str(row_idx + 1)

    # ── Type-based generation ──
    if st in ("INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT"):
        if HAS_FAKER:
            return str(_fake.random_int(1, 10000))
        return str(random.randint(1, 10000))

    if st in ("FLOAT", "DOUBLE", "REAL", "DECIMAL", "NUMERIC", "MONEY"):
        if HAS_FAKER:
            return f"{_fake.pyfloat(min_value=0.01, max_value=99999.99, right_digits=2)}"
        return f"{random.uniform(0.01, 99999.99):.2f}"

    if st in ("BOOLEAN", "BIT"):
        return random.choice(["TRUE", "FALSE"])

    if st == "DATE":
        if HAS_FAKER:
            d = _fake.date_between(start_date='-4y', end_date='today')
            return f"DATE '{d.strftime('%Y-%m-%d')}'"
        d = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1500))
        return f"DATE '{d.strftime('%Y-%m-%d')}'"

    if st in ("TIMESTAMP", "DATETIME", "DATETIME2"):
        if HAS_FAKER:
            d = _fake.date_time_between(start_date='-4y', end_date='now')
            return f"TIMESTAMP '{d.strftime('%Y-%m-%d %H:%M:%S')}'"
        d = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1500), hours=random.randint(0, 23))
        return f"TIMESTAMP '{d.strftime('%Y-%m-%d %H:%M:%S')}'"

    if st in ("BLOB", "VARBINARY", "IMAGE"):
        return "NULL"

    # ── Name-based Faker generation (VARCHAR/unknown) ──
    if HAS_FAKER:
        return _faker_by_name(name, row_idx)
    else:
        return _fallback_by_name(name, row_idx)


def _faker_by_name(name, row_idx):
    """Use Faker to generate realistic values based on column name patterns."""
    # Person
    if "firstname" in name or "first_name" in name:
        return f"'{_fake.first_name()}'"
    if "lastname" in name or "last_name" in name:
        return f"'{_fake.last_name()}'"
    if "middlename" in name or "middle_name" in name:
        return f"'{_fake.first_name()[0]}'"
    if "fullname" in name or "name" in name:
        return f"'{_fake.name()}'"
    if "title" in name and "job" not in name:
        return f"'{random.choice(['Mr.', 'Ms.', 'Dr.', 'Mrs.'])}'"
    if "suffix" in name:
        return f"'{random.choice(['Jr.', 'Sr.', 'III', ''])}'"
    if "gender" in name or "sex" in name:
        return f"'{random.choice(['M', 'F'])}'"

    # Contact
    if "email" in name:
        return f"'{_fake.email()}'"
    if "phone" in name or "mobile" in name or "fax" in name:
        return f"'{_fake.phone_number()[:15]}'"

    # Address
    if "address" in name and ("line" in name or "1" in name or "2" in name):
        return f"'{_fake.street_address()}'"
    if "address" in name:
        return f"'{_fake.address().split(chr(10))[0]}'"
    if "city" in name:
        return f"'{_fake.city()}'"
    if "state" in name or "province" in name or "region" in name:
        return f"'{_fake.state_abbr()}'"
    if "zip" in name or "postal" in name:
        return f"'{_fake.zipcode()}'"
    if "country" in name:
        return f"'{_fake.country_code()}'"

    # Business
    if "company" in name or "vendor" in name or "supplier" in name:
        return f"'{_fake.company()[:40]}'"
    if "department" in name or "dept" in name:
        return f"'{random.choice(['Engineering', 'Sales', 'Marketing', 'Finance', 'HR', 'Operations'])}'"
    if "jobtitle" in name or "job_title" in name or "position" in name:
        return f"'{_fake.job()[:30]}'"

    # Financial
    if "cost" in name or "price" in name or "amount" in name or "total" in name:
        return f"{_fake.pyfloat(min_value=1.0, max_value=50000.0, right_digits=2)}"
    if "rate" in name:
        return f"{_fake.pyfloat(min_value=0.01, max_value=100.0, right_digits=4)}"
    if "qty" in name or "quantity" in name or "count" in name:
        return str(_fake.random_int(1, 500))
    if "weight" in name or "mass" in name:
        return f"{_fake.pyfloat(min_value=0.1, max_value=1000.0, right_digits=2)}"
    if "hrs" in name or "hours" in name:
        return f"{_fake.pyfloat(min_value=0.5, max_value=40.0, right_digits=1)}"

    # Status/Type
    if "status" in name:
        return f"'{random.choice(['Active', 'Inactive', 'Pending', 'Completed', 'Cancelled'])}'"
    if "type" in name or "category" in name:
        return f"'{random.choice(['Type_A', 'Type_B', 'Type_C', 'Standard', 'Premium'])}'"
    if "flag" in name or "is_" in name or "has_" in name:
        return random.choice(["'Y'", "'N'"])
    if "priority" in name:
        return f"'{random.choice(['High', 'Medium', 'Low'])}'"

    # Description/Text
    if "description" in name or "comment" in name or "note" in name or "remark" in name:
        return f"'{_fake.sentence(nb_words=6)[:60]}'"
    if "reason" in name:
        return f"'{random.choice(['Quality issue', 'Cost reduction', 'Schedule conflict', 'Resource limit', 'Client request'])}'"
    if "url" in name or "website" in name:
        return f"'{_fake.url()[:50]}'"

    # Dates as strings
    if "date" in name:
        return f"'{_fake.date_between(start_date='-4y', end_date='today').strftime('%Y-%m-%d')}'"
    if "time" in name:
        return f"'{_fake.time()}'"

    # Measurement/UOM
    if "measure" in name or "uom" in name or "unit" in name:
        return f"'{random.choice(['EA', 'KG', 'LB', 'PC', 'BOX', 'CTN'])}'"
    if "color" in name or "colour" in name:
        return f"'{_fake.color_name()}'"
    if "size" in name:
        return f"'{random.choice(['S', 'M', 'L', 'XL', 'XXL'])}'"

    # Tool/system
    if "tool" in name or "system" in name or "source" in name:
        return f"'{random.choice(['Talend', 'dbt', 'SSIS', 'Informatica', 'Manual'])}'"
    if "version" in name:
        return f"'{_fake.random_int(1, 10)}.{_fake.random_int(0, 9)}'"
    if "user" in name or "login" in name or "created_by" in name or "modified_by" in name:
        return f"'{_fake.user_name()[:20]}'"

    # Generic fallback with Faker
    return f"'{_fake.word().capitalize()}_{row_idx + 1}'"


def _fallback_by_name(name, row_idx):
    """Fallback without Faker — still smarter than v3."""
    if "name" in name:
        names = ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'Zeta', 'Eta', 'Theta']
        return f"'Test_{names[row_idx % len(names)]}_{row_idx + 1}'"
    if "email" in name:
        return f"'user{row_idx + 1}@test.com'"
    if "phone" in name:
        return f"'555-{random.randint(1000, 9999)}'"
    if "city" in name:
        cities = ["'New York'", "'Boston'", "'Chicago'", "'Seattle'", "'Austin'"]
        return cities[row_idx % len(cities)]
    if "country" in name:
        return random.choice(["'US'", "'UK'", "'CA'", "'DE'", "'IN'"])
    if "status" in name:
        return random.choice(["'Active'", "'Inactive'", "'Pending'"])
    if "description" in name or "desc" in name:
        return f"'Description for item {row_idx + 1}'"
    if "cost" in name or "price" in name or "amount" in name:
        return f"{random.uniform(10, 5000):.2f}"
    if "date" in name:
        d = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1500))
        return f"'{d.strftime('%Y-%m-%d')}'"
    if "tool" in name:
        return "'Talend'"
    return f"'val_{col.name[:12]}_{row_idx + 1}'"


# =====================================================================
# Source Catalog Builder
# =====================================================================

def _build_source_map(project):
    source_map = {}
    for sid, src in project.source_catalog.items():
        schema = "raw"
        if src.connection and src.connection.database:
            schema = src.connection.database.replace('"', '').replace("'", "")
            schema = schema.replace("-", "_").replace(".", "_")
        table = ""
        if src.connection and src.connection.table:
            table = src.connection.table.replace('"', '').replace("'", "")
            table = table.split(".")[-1]
        if not table:
            table = sid
        source_map.setdefault(schema, {})[table] = src.columns or []

    for job in project.jobs.values():
        for comp in job.components.values():
            if not comp.schemas: continue
            if comp.behavior.value not in ("DATA_SOURCE", "DATA_SINK"): continue
            for conn, cols in comp.schemas.items():
                if conn == "REJECT" or not cols: continue
                table = comp.parameters.get("TABLE", "").replace('"', '').replace("'", "")
                table = table.split(".")[-1] if "." in table else table
                if not table: table = comp.unique_name
                db = ""
                if comp.source_info and comp.source_info.connection:
                    db = (comp.source_info.connection.database or "").replace('"', '').replace("'", "")
                    db = db.replace("-", "_").replace(".", "_")
                if not db: db = "raw"
                if table not in source_map.get(db, {}):
                    source_map.setdefault(db, {})[table] = cols
    return source_map


# =====================================================================
# FK Graph
# =====================================================================

def _build_fk_graph(output_dir):
    fk_edges = []
    models_dir = Path(output_dir) / "models"
    if not models_dir.exists(): return fk_edges

    for sf in models_dir.rglob("*.sql"):
        try:
            content = sf.read_text(encoding="utf-8")
            cte_to_source = {}
            for m in re.finditer(
                r"(\w+)\s+AS\s*\(\s*SELECT\s+\*\s+FROM\s+\{\{\s*source\(\s*'(\w+)'\s*,\s*'(\w+)'\s*\)\s*\}\}",
                content, re.IGNORECASE
            ):
                cte_to_source[m.group(1).lower()] = (m.group(2), m.group(3))
            for m in re.finditer(
                r'JOIN\s+(\w+)\s+ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)',
                content, re.IGNORECASE
            ):
                lcte, lcol = m.group(2).lower(), m.group(3)
                rcte, rcol = m.group(4).lower(), m.group(5)
                lsrc, rsrc = cte_to_source.get(lcte), cte_to_source.get(rcte)
                if lsrc and rsrc:
                    fk_edges.append((f"{lsrc[0]}.{lsrc[1]}", lcol, f"{rsrc[0]}.{rsrc[1]}", rcol))
        except: pass
    return fk_edges


# =====================================================================
# Column Reconciliation
# =====================================================================

def _reconcile_columns(con, output_dir):
    models_dir = Path(output_dir) / "models"
    if not models_dir.exists(): return
    table_needs = defaultdict(set)

    for sf in models_dir.rglob("*.sql"):
        try:
            content = sf.read_text(encoding="utf-8")
            cte_to_source = {}
            for m in re.finditer(
                r"(\w+)\s+AS\s*\(\s*SELECT\s+\*\s+FROM\s+\{\{\s*source\(\s*'(\w+)'\s*,\s*'(\w+)'\s*\)\s*\}\}",
                content, re.IGNORECASE
            ):
                cte_to_source[m.group(1).lower()] = (m.group(2), m.group(3))
            for m in re.finditer(r'\b(\w+)\.(\w+)\b', content):
                cte = m.group(1).lower()
                col = m.group(2)
                src = cte_to_source.get(cte)
                if src: table_needs[f"{src[0]}.{src[1]}"].add(col)
        except: pass

    for qualified, needed_cols in table_needs.items():
        schema, table = qualified.split(".", 1)
        existing = set()
        try:
            for row in con.execute(
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_schema='{schema}' AND table_name='{table}'"
            ).fetchall():
                existing.add(row[0].lower())
        except: continue
        for col in needed_cols:
            if col.lower() not in existing:
                try:
                    con.execute(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN "{col}" VARCHAR DEFAULT NULL')
                except: pass


# =====================================================================
# FK Value Patching
# =====================================================================

def _patch_fk_values(con, output_dir):
    fk_edges = _build_fk_graph(output_dir)
    if not fk_edges: return

    for lqual, lcol, rqual, rcol in fk_edges:
        lschema, ltable = lqual.split(".", 1)
        rschema, rtable = rqual.split(".", 1)
        try:
            lvals = [row[0] for row in con.execute(
                f'SELECT DISTINCT "{lcol}" FROM "{lschema}"."{ltable}" WHERE "{lcol}" IS NOT NULL LIMIT 20'
            ).fetchall()]
            if not lvals: continue
            rvals = [row[0] for row in con.execute(
                f'SELECT DISTINCT "{rcol}" FROM "{rschema}"."{rtable}" WHERE "{rcol}" IS NOT NULL LIMIT 20'
            ).fetchall()]
            if set(str(v) for v in lvals) & set(str(v) for v in rvals): continue

            rcols = [row[0] for row in con.execute(
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_schema='{rschema}' AND table_name='{rtable}' ORDER BY ordinal_position"
            ).fetchall()]
            if not rcols: continue
            rows = con.execute(f'SELECT * FROM "{rschema}"."{rtable}"').fetchall()
            if not rows: continue

            col_idx = None
            for ci, cn in enumerate(rcols):
                if cn.lower() == rcol.lower(): col_idx = ci; break
            if col_idx is None: continue

            con.execute(f'DELETE FROM "{rschema}"."{rtable}"')
            for ri, row in enumerate(rows):
                new_row = list(row)
                new_row[col_idx] = lvals[ri % len(lvals)]
                placeholders = ', '.join(['?'] * len(new_row))
                col_list = ', '.join(f'"{c}"' for c in rcols)
                try:
                    con.execute(f'INSERT INTO "{rschema}"."{rtable}" ({col_list}) VALUES ({placeholders})', new_row)
                except:
                    new_row[col_idx] = str(lvals[ri % len(lvals)])
                    try:
                        con.execute(f'INSERT INTO "{rschema}"."{rtable}" ({col_list}) VALUES ({placeholders})', new_row)
                    except: pass
        except: pass


# =====================================================================
# Main: Load into DuckDB
# =====================================================================

def load_test_data_into_duckdb(project, db_path, row_count=20):
    from taldbt.engine.duckdb_engine import create_connection
    random.seed(42)
    if HAS_FAKER: Faker.seed(42)

    con = create_connection(db_path)
    # Report flock status
    from taldbt.engine.duckdb_engine import check_flock
    flock_ok = check_flock(con)

    tables_created = 0
    total_rows = 0
    errors = []

    source_map = _build_source_map(project)

    for schema, tables in source_map.items():
        try:
            con.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        except Exception as e:
            errors.append(f"Schema {schema}: {e}"); continue

        for table, columns in tables.items():
            try:
                seen = set()
                deduped = []
                for c in columns:
                    if c.name.lower() not in seen:
                        seen.add(c.name.lower()); deduped.append(c)
                columns = deduped

                if not columns:
                    con.execute(f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" ("_placeholder" VARCHAR)')
                    tables_created += 1; continue

                col_defs = ", ".join(f'"{c.name}" {c.sql_type or "VARCHAR"}' for c in columns)
                con.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}"')
                con.execute(f'CREATE TABLE "{schema}"."{table}" ({col_defs})')

                for i in range(row_count):
                    vals = []
                    for c in columns:
                        if c.nullable and not c.is_key and random.random() < 0.05:
                            vals.append("NULL")
                        else:
                            vals.append(_gen_value(c, i))
                    vals_str = ", ".join(vals)
                    col_list = ", ".join(f'"{c.name}"' for c in columns)
                    con.execute(f'INSERT INTO "{schema}"."{table}" ({col_list}) VALUES ({vals_str})')

                total_rows += row_count
                tables_created += 1
            except Exception as e:
                errors.append(f"{schema}.{table}: {str(e)[:120]}")

    output_dir = str(Path(db_path).parent)
    try: _reconcile_columns(con, output_dir)
    except Exception as e: errors.append(f"Reconcile: {str(e)[:120]}")
    try: _patch_fk_values(con, output_dir)
    except Exception as e: errors.append(f"FK patch: {str(e)[:120]}")

    con.close()
    return {
        "tables_created": tables_created, "total_rows": total_rows,
        "errors": errors, "db_path": db_path,
        "flock_available": flock_ok,
        "faker_available": HAS_FAKER,
    }


# =====================================================================
# CSV + SQL + File utilities (preserved from v3)
# =====================================================================

def write_test_data_sql(project, output_path, row_count=20):
    random.seed(42)
    if HAS_FAKER: Faker.seed(42)
    source_map = _build_source_map(project)
    out = Path(output_path); out.mkdir(parents=True, exist_ok=True)
    sql_path = out / "test_data.sql"
    lines = [f"-- taldbt Test Data (Faker={'yes' if HAS_FAKER else 'no'}): {datetime.now()}", ""]
    for schema, tables in sorted(source_map.items()):
        lines.append(f'CREATE SCHEMA IF NOT EXISTS "{schema}";'); lines.append("")
        for table, columns in sorted(tables.items()):
            seen = set()
            deduped = [c for c in columns if c.name.lower() not in seen and not seen.add(c.name.lower())]
            if not deduped: continue
            col_defs = ", ".join(f'"{c.name}" {c.sql_type or "VARCHAR"}' for c in deduped)
            lines.append(f'CREATE TABLE "{schema}"."{table}" ({col_defs});')
            rows = []
            for i in range(row_count):
                vals = [_gen_value(c, i) for c in deduped]
                rows.append(f"  ({', '.join(vals)})")
            col_list = ", ".join(f'"{c.name}"' for c in deduped)
            lines.append(f'INSERT INTO "{schema}"."{table}" ({col_list}) VALUES')
            lines.append(",\n".join(rows) + ";"); lines.append("")
    sql_path.write_text("\n".join(lines), encoding="utf-8")
    return str(sql_path)


def _gen_csv_value(col, row_idx):
    st = (col.sql_type or "VARCHAR").upper().split("(")[0]
    if _is_joinable_col(col): return str(row_idx + 1)
    if st in ("INTEGER", "BIGINT", "SMALLINT", "TINYINT"):
        return str(_fake.random_int(1, 100)) if HAS_FAKER else str(random.randint(1, 100))
    if st in ("FLOAT", "DOUBLE", "DECIMAL"):
        return f"{random.uniform(0.01, 9999.99):.2f}"
    if st == "BOOLEAN": return random.choice(["true", "false"])
    if st == "DATE":
        if HAS_FAKER: return _fake.date_between(start_date='-4y', end_date='today').strftime("%Y-%m-%d")
        return (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1500))).strftime("%Y-%m-%d")
    if "name" in col.name.lower():
        return _fake.name() if HAS_FAKER else f"Test_{row_idx+1}"
    if HAS_FAKER: return _fake.word()
    return f"test_{col.name[:10]}_{row_idx + 1}"


def generate_file_sources(project, output_dir, row_count=20):
    random.seed(42)
    if HAS_FAKER: Faker.seed(42)
    test_dir = Path(output_dir) / "test_data"
    test_dir.mkdir(parents=True, exist_ok=True)
    path_map = {}; seen = set()
    for job in project.jobs.values():
        for comp in job.components.values():
            si = comp.source_info
            if not si or not si.file_path or si.file_path in seen: continue
            seen.add(si.file_path)
            cols = si.columns
            if not cols and comp.schemas:
                for cn, sc in comp.schemas.items():
                    if cn != "REJECT" and sc: cols = sc; break
            if not cols:
                cols = [ColumnSchema(name="Column0", sql_type="VARCHAR"), ColumnSchema(name="Column1", sql_type="VARCHAR")]
            data_cols = [c for c in cols if c.name.lower() not in ("errorcode", "errormessage")] or cols[:2]
            csv_path = (test_dir / Path(si.file_path).name).with_suffix(".csv")
            try:
                import csv
                with open(csv_path, "w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    w.writerow([c.name for c in data_cols])
                    for i in range(row_count):
                        w.writerow([_gen_csv_value(c, i) for c in data_cols])
                path_map[si.file_path] = str(csv_path)
            except Exception as e:
                path_map[si.file_path] = f"ERROR: {e}"
    return path_map


def rewrite_file_paths_in_models(output_dir, path_map):
    models_dir = Path(output_dir) / "models"
    if not models_dir.exists(): return 0
    bm = {}
    for op, lp in path_map.items():
        if "ERROR" in str(lp): continue
        fwd = str(lp).replace("\\", "/")
        bm[Path(op).name.lower()] = fwd
        bm[(Path(op).stem + ".csv").lower()] = fwd
    if not bm: return 0
    rewritten = 0
    for sf in models_dir.rglob("*.sql"):
        try:
            content = sf.read_text(encoding="utf-8"); orig = content
            def _rep(m):
                fp = m.group(2)
                local = bm.get(Path(fp).name.lower()) or bm.get((Path(fp).stem + ".csv").lower())
                return f"read_csv('{local}', header=true, auto_detect=true)" if local else m.group(0)
            content = re.sub(r"(read_csv_auto|read_csv|read_excel)\s*\(\s*'([^']+)'[^)]*\)", _rep, content)
            if content != orig: sf.write_text(content, encoding="utf-8"); rewritten += 1
        except: pass
    return rewritten
