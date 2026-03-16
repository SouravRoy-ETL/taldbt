"""
Knowledge Base: deterministic mapping of Talend Java expressions to DuckDB SQL.
Handles ~85% of expressions without LLM using a 6-stage translation pipeline:
  1. System variable replacement (Pid, rootPid, jobName, etc.)
  2. Static zero-arg routine replacement (TalendDate.getCurrentDate(), etc.)
  3. Function-with-args pattern replacement (TalendDate.formatDate("fmt", x), etc.)
  4. Java method call translation (.toUpperCase(), .substring(), etc.)
  5. Java operator normalization (==, &&, ||, !=null, ==null, ternary)
  6. Java literal normalization ("string" → 'string', null → NULL)
"""
from __future__ import annotations
import re
from typing import Optional
from taldbt.models.ast_models import ExpressionStrategy


# ═══════════════════════════════════════════════════════════
# STAGE 1: Talend System Variables
# These are instance fields on the generated Java job class.
# ═══════════════════════════════════════════════════════════

SYSTEM_VARS: dict[str, str] = {
    "pid":          "'{{ invocation_id }}'",
    "Pid":          "'{{ invocation_id }}'",
    "rootPid":      "'{{ invocation_id }}'",
    "root_pid":     "'{{ invocation_id }}'",
    "fatherPid":    "'{{ invocation_id }}'",
    "father_pid":   "'{{ invocation_id }}'",
    "jobName":      "'{{ this.name }}'",
    "projectName":  "'{{ project_name }}'",
    "contextStr":   "'{{ target.name }}'",
    "jobVersion":   "'1.0'",
    "currentDate":  "CURRENT_DATE",
    "startTime":    "CURRENT_TIMESTAMP",
    "isChildJob":   "FALSE",
    "talpieces_root_dir":  "''",
}


def _replace_system_vars(expr: str) -> str:
    """Replace standalone Talend system variables with dbt/SQL equivalents."""
    result = expr
    for java_var, sql_val in SYSTEM_VARS.items():
        # Match as standalone token (not part of row1.Pid, just bare Pid)
        # Negative lookbehind: not preceded by . or "
        result = re.sub(
            rf'(?<![.\w"])(?<!\w){re.escape(java_var)}(?!\w)',
            sql_val,
            result,
        )
    return result


# ═══════════════════════════════════════════════════════════
# STAGE 2: Static zero-arg routine calls
# ═══════════════════════════════════════════════════════════

_STATIC_ROUTINES: dict[str, str] = {
    # TalendDate
    "TalendDate.getCurrentDate()":  "CURRENT_TIMESTAMP",
    "TalendDate.getDate()":         "CURRENT_TIMESTAMP",
    # Numeric
    "Numeric.random()":             "(FLOOR(RANDOM() * 2147483647))::INTEGER",
    # System
    "System.currentTimeMillis()":   "EPOCH_MS(CURRENT_TIMESTAMP)",
    "System.getProperty(\"os.name\")":  "'Linux'",
}


def _replace_static_routines(expr: str) -> tuple[str, bool]:
    """Replace zero-arg routine calls. Returns (result, was_changed)."""
    result = expr
    changed = False
    for java_call, sql_equiv in _STATIC_ROUTINES.items():
        # Handle trailing whitespace in expressions (e.g. "TalendDate.getCurrentDate() ")
        pattern = re.escape(java_call).replace(r'\(', r'\(\s*').replace(r'\)', r'\s*\)')
        new = re.sub(pattern, sql_equiv, result)
        if new != result:
            result = new
            changed = True
    return result, changed


# ═══════════════════════════════════════════════════════════
# STAGE 3: Function-with-args pattern matching
# Order matters: more specific patterns first.
# Each entry: (regex, replacement_fn, description)
# ═══════════════════════════════════════════════════════════

def _java_date_fmt_to_duckdb(fmt: str) -> str:
    """Convert Java SimpleDateFormat pattern to DuckDB strftime format.

    Uses placeholder approach to prevent double-replacement:
    longer patterns are replaced first, and already-replaced %X tokens
    are protected from shorter patterns (e.g., 'dd' → '%d' won't get
    hit again by single 'd' → '%-d').
    """
    # Ordered longest-first within each letter group to prevent partial matches
    mapping = [
        # Year
        ("yyyy", "%Y"), ("yy", "%y"),
        # Month (uppercase M = month, lowercase m = minute)
        ("MMMM", "%B"), ("MMM", "%b"), ("MM", "%m"),
        # Day
        ("dd", "%d"),
        # Hour
        ("HH", "%H"), ("hh", "%I"),
        # Minute (lowercase m)
        ("mm", "%M"),
        # Second
        ("ss", "%S"), ("SSS", "%g"),
        # AM/PM
        ("a", "%p"),
        # Day of week
        ("EEEE", "%A"), ("EEE", "%a"),
        # Day of year
        ("DD", "%j"),
    ]

    # Use placeholders to prevent double-replacement
    result = fmt
    placeholders = {}
    for i, (java_pat, duck_pat) in enumerate(mapping):
        placeholder = f"\x00{i:02d}\x00"
        if java_pat in result:
            result = result.replace(java_pat, placeholder, 1)
            placeholders[placeholder] = duck_pat

    # Replace single-char patterns only if they haven't been consumed
    single_char_map = [
        ("d", "%-d"), ("M", "%-m"), ("H", "%-H"),
        ("h", "%-I"), ("m", "%-M"), ("s", "%-S"),
    ]
    for java_pat, duck_pat in single_char_map:
        if java_pat in result:
            placeholder = f"\x00S{java_pat}\x00"
            result = result.replace(java_pat, placeholder, 1)
            placeholders[placeholder] = duck_pat

    # Restore all placeholders
    for ph, val in placeholders.items():
        result = result.replace(ph, val)

    return result


_FUNC_PATTERNS: list[tuple[re.Pattern, callable]] = [
    # ── TalendDate ────────────────────────────────────
    # TalendDate.formatDate("yyyy-MM-dd", expr)
    (re.compile(r'TalendDate\.formatDate\s*\(\s*"([^"]+)"\s*,\s*(.+?)\s*\)'),
     lambda m: f"STRFTIME({m.group(2)}, '{_java_date_fmt_to_duckdb(m.group(1))}')"),

    # TalendDate.parseDate("fmt", stringExpr)
    (re.compile(r'TalendDate\.parseDate\s*\(\s*"([^"]+)"\s*,\s*(.+?)\s*\)'),
     lambda m: f"STRPTIME({m.group(2)}, '{_java_date_fmt_to_duckdb(m.group(1))}')"),

    # TalendDate.parseDateLocale("fmt", str, "locale")
    (re.compile(r'TalendDate\.parseDateLocale\s*\(\s*"([^"]+)"\s*,\s*(.+?)\s*,\s*"[^"]*"\s*\)'),
     lambda m: f"STRPTIME({m.group(2)}, '{_java_date_fmt_to_duckdb(m.group(1))}')"),

    # TalendDate.addDate(date, n, "UNIT")
    (re.compile(r'TalendDate\.addDate\s*\(\s*(.+?)\s*,\s*(\-?\d+)\s*,\s*"(\w+)"\s*\)'),
     lambda m: f"({m.group(1)} + INTERVAL '{m.group(2)}' {m.group(3).upper()})"),

    # TalendDate.ADD_TO_DATE(date, "UNIT", amount)
    (re.compile(r'TalendDate\.ADD_TO_DATE\s*\(\s*(.+?)\s*,\s*"(\w+)"\s*,\s*(\-?\d+)\s*\)'),
     lambda m: f"({m.group(1)} + INTERVAL '{m.group(3)}' {m.group(2).upper()})"),

    # TalendDate.diffDate(date1, date2, "UNIT")
    (re.compile(r'TalendDate\.diffDate\s*\(\s*(.+?)\s*,\s*(.+?)\s*,\s*"(\w+)"\s*\)'),
     lambda m: f"DATE_DIFF('{m.group(3).lower()}', {m.group(2)}, {m.group(1)})"),

    # TalendDate.diffDateFloor(date1, date2, "UNIT")
    (re.compile(r'TalendDate\.diffDateFloor\s*\(\s*(.+?)\s*,\s*(.+?)\s*,\s*"(\w+)"\s*\)'),
     lambda m: f"DATE_DIFF('{m.group(3).lower()}', {m.group(2)}, {m.group(1)})"),

    # TalendDate.compareDate(d1, d2, "fmt") — 3-arg version with format (seen 6+ times in real filters)
    (re.compile(r'TalendDate\.compareDate\s*\(\s*(.+?)\s*,\s*(.+?)\s*,\s*"[^"]*"\s*\)'),
     lambda m: f"CASE WHEN CAST({m.group(1)} AS DATE) < CAST({m.group(2)} AS DATE) THEN -1 WHEN CAST({m.group(1)} AS DATE) = CAST({m.group(2)} AS DATE) THEN 0 ELSE 1 END"),

    # TalendDate.compareDate(d1, d2) — 2-arg version
    (re.compile(r'TalendDate\.compareDate\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)'),
     lambda m: f"CASE WHEN {m.group(1)} < {m.group(2)} THEN -1 WHEN {m.group(1)} = {m.group(2)} THEN 0 ELSE 1 END"),

    # TalendDate.getPartOfDate("PART", date) — NOTE: MONTH is 0-based in Talend!
    (re.compile(r'TalendDate\.getPartOfDate\s*\(\s*"(\w+)"\s*,\s*(.+?)\s*\)'),
     lambda m: (
         f"(EXTRACT(MONTH FROM {m.group(2)}) - 1)" if m.group(1).upper() == "MONTH"
         else f"EXTRACT({m.group(1).upper()} FROM {m.group(2)})"
     )),

    # TalendDate.getFirstDayOfMonth / getFirstDayMonth (both names exist)
    (re.compile(r'TalendDate\.getFirstDay(?:Of)?Month\s*\(\s*(.+?)\s*\)'),
     lambda m: f"DATE_TRUNC('month', {m.group(1)})"),

    # TalendDate.getLastDayOfMonth / getLastDayMonth
    (re.compile(r'TalendDate\.getLastDay(?:Of)?Month\s*\(\s*(.+?)\s*\)'),
     lambda m: f"LAST_DAY({m.group(1)})"),

    # TalendDate.formatDateLocale("fmt", date, "locale")
    (re.compile(r'TalendDate\.formatDateLocale\s*\(\s*"([^"]+)"\s*,\s*(.+?)\s*,\s*"[^"]*"\s*\)'),
     lambda m: f"STRFTIME({m.group(2)}, '{_java_date_fmt_to_duckdb(m.group(1))}')"),

     # TalendDate.isDate(str, "fmt")
    (re.compile(r'TalendDate\.isDate\s*\(\s*(.+?)\s*,\s*"([^"]+)"\s*\)'),
     lambda m: f"(TRY_STRPTIME({m.group(1)}, '{_java_date_fmt_to_duckdb(m.group(2))}') IS NOT NULL)"),

    # TalendDate.TO_DATE(str, "fmt")
    (re.compile(r'TalendDate\.TO_DATE\s*\(\s*(.+?)\s*,\s*"([^"]+)"\s*\)'),
     lambda m: f"STRPTIME({m.group(1)}, '{_java_date_fmt_to_duckdb(m.group(2))}')"),

    # TalendDate.TO_CHAR(date, "fmt")
    (re.compile(r'TalendDate\.TO_CHAR\s*\(\s*(.+?)\s*,\s*"([^"]+)"\s*\)'),
     lambda m: f"STRFTIME({m.group(1)}, '{_java_date_fmt_to_duckdb(m.group(2))}')"),

    # ── StringHandling ────────────────────────────────
    (re.compile(r'StringHandling\.UPCASE\s*\(\s*(.+?)\s*\)'),    lambda m: f"UPPER({m.group(1)})"),
    (re.compile(r'StringHandling\.DOWNCASE\s*\(\s*(.+?)\s*\)'),  lambda m: f"LOWER({m.group(1)})"),
    (re.compile(r'StringHandling\.TRIM\s*\(\s*(.+?)\s*\)'),      lambda m: f"TRIM({m.group(1)})"),
    (re.compile(r'StringHandling\.LTRIM\s*\(\s*(.+?)\s*\)'),     lambda m: f"LTRIM({m.group(1)})"),
    (re.compile(r'StringHandling\.RTRIM\s*\(\s*(.+?)\s*\)'),     lambda m: f"RTRIM({m.group(1)})"),
    (re.compile(r'StringHandling\.LEN\s*\(\s*(.+?)\s*\)'),       lambda m: f"LENGTH({m.group(1)})"),
    (re.compile(r'StringHandling\.LEFT\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)'),  lambda m: f"LEFT({m.group(1)}, {m.group(2)})"),
    (re.compile(r'StringHandling\.RIGHT\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)'), lambda m: f"RIGHT({m.group(1)}, {m.group(2)})"),
    (re.compile(r'StringHandling\.SUBSTR\s*\(\s*(.+?)\s*,\s*(.+?)\s*,\s*(.+?)\s*\)'),
     lambda m: f"SUBSTRING({m.group(1)}, {m.group(2)} + 1, {m.group(3)})"),
    (re.compile(r'StringHandling\.INDEX\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)'),
     lambda m: f"(STRPOS({m.group(1)}, {m.group(2)}) - 1)"),
    (re.compile(r'StringHandling\.CHANGE\s*\(\s*(.+?)\s*,\s*(.+?)\s*,\s*(.+?)\s*\)'),
     lambda m: f"REPLACE({m.group(1)}, {m.group(2)}, {m.group(3)})"),
    (re.compile(r'StringHandling\.EREPLACE\s*\(\s*(.+?)\s*,\s*(.+?)\s*,\s*(.+?)\s*\)'),
     lambda m: f"REGEXP_REPLACE({m.group(1)}, {m.group(2)}, {m.group(3)}, 'g')"),
    (re.compile(r'StringHandling\.COUNT\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)'),
     lambda m: f"((LENGTH({m.group(1)}) - LENGTH(REPLACE({m.group(1)}, {m.group(2)}, ''))) / LENGTH({m.group(2)}))"),
    (re.compile(r'StringHandling\.LPAD\s*\(\s*(.+?)\s*,\s*(.+?)\s*,\s*(.+?)\s*\)'),
     lambda m: f"LPAD({m.group(1)}, {m.group(2)}, {m.group(3)})"),
    (re.compile(r'StringHandling\.RPAD\s*\(\s*(.+?)\s*,\s*(.+?)\s*,\s*(.+?)\s*\)'),
     lambda m: f"RPAD({m.group(1)}, {m.group(2)}, {m.group(3)})"),
    (re.compile(r'StringHandling\.IS_ALPHA\s*\(\s*(.+?)\s*\)'),
     lambda m: f"REGEXP_MATCHES({m.group(1)}, '^[a-zA-Z]+$')"),
    (re.compile(r'StringHandling\.SPACE\s*\(\s*(.+?)\s*\)'),     lambda m: f"REPEAT(' ', {m.group(1)})"),
    (re.compile(r'StringHandling\.DQUOTE\s*\(\s*(.+?)\s*\)'),    lambda m: f"('\"' || {m.group(1)} || '\"')"),
    (re.compile(r'StringHandling\.SQUOTE\s*\(\s*(.+?)\s*\)'),    lambda m: f"('\\'' || {m.group(1)} || '\\'')"),

    # ── Relational ────────────────────────────────────
    (re.compile(r'Relational\.ISNULL\s*\(\s*(.+?)\s*\)'),     lambda m: f"({m.group(1)} IS NULL)"),
    (re.compile(r'Relational\.NOT\s*\(\s*(.+?)\s*\)'),        lambda m: f"(NOT {m.group(1)})"),

    # ── Numeric ───────────────────────────────────────
    # Numeric.sequence("name", start, step) → ROW_NUMBER()
    # Also handle cast prefix: (int)Numeric.sequence(...), (long)Numeric.sequence(...)
    # NOTE: closing paren optional — some Talend XML has malformed expressions missing it
    (re.compile(r'(?:\(\s*(?:int|long|Integer|Long)\s*\)\s*)?Numeric\.sequence\s*\([^)]*\)?'),
     lambda m: "ROW_NUMBER() OVER ()"),
    # Numeric.random(min, max) → FLOOR(RANDOM() * (max-min+1) + min)
    (re.compile(r'Numeric\.random\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)'),
     lambda m: f"FLOOR(RANDOM() * ({m.group(2)} - {m.group(1)} + 1) + {m.group(1)})::INTEGER"),

    # ── Mathematical ──────────────────────────────────
    (re.compile(r'Mathematical\.ABS\s*\(\s*(.+?)\s*\)'),   lambda m: f"ABS({m.group(1)})"),
    (re.compile(r'Mathematical\.INT\s*\(\s*(.+?)\s*\)'),   lambda m: f"({m.group(1)})::INTEGER"),
    (re.compile(r'Mathematical\.SQRT\s*\(\s*(.+?)\s*\)'),  lambda m: f"SQRT({m.group(1)})"),
    (re.compile(r'Mathematical\.MOD\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)'),
     lambda m: f"({m.group(1)} % {m.group(2)})"),
    (re.compile(r'Mathematical\.POW\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)'),
     lambda m: f"POWER({m.group(1)}, {m.group(2)})"),
    (re.compile(r'Mathematical\.CEIL\s*\(\s*(.+?)\s*\)'),  lambda m: f"CEIL({m.group(1)})"),
    (re.compile(r'Mathematical\.FLOOR\s*\(\s*(.+?)\s*\)'), lambda m: f"FLOOR({m.group(1)})"),
    (re.compile(r'Mathematical\.ROUND\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)'),
     lambda m: f"ROUND({m.group(1)}, {m.group(2)})"),
    (re.compile(r'Mathematical\.LOG\s*\(\s*(.+?)\s*\)'),   lambda m: f"LN({m.group(1)})"),
    (re.compile(r'Mathematical\.LOG10\s*\(\s*(.+?)\s*\)'), lambda m: f"LOG10({m.group(1)})"),

    # ── DataOperation ─────────────────────────────────
    (re.compile(r'DataOperation\.CHAR\s*\(\s*(.+?)\s*\)'),  lambda m: f"CHR({m.group(1)})"),
    (re.compile(r'DataOperation\.DTX\s*\(\s*(.+?)\s*\)'),   lambda m: f"PRINTF('%x', {m.group(1)})"),
    (re.compile(r'DataOperation\.FIX\s*\(\s*(.+?)\s*\)'),   lambda m: f"({m.group(1)})::BIGINT"),
    (re.compile(r'DataOperation\.XTD\s*\(\s*(.+?)\s*\)'),   lambda m: f"('0x' || {m.group(1)})::BIGINT"),

    # ── TalendString ──────────────────────────────────
    (re.compile(r'TalendString\.talpieces_replaceAll\s*\(\s*(.+?)\s*,\s*(.+?)\s*,\s*(.+?)\s*\)'),
     lambda m: f"REGEXP_REPLACE({m.group(1)}, {m.group(2)}, {m.group(3)}, 'g')"),
    (re.compile(r'TalendStringUtil\.addQuotes4SQLIn\s*\(\s*(.+?)\s*\)'),
     lambda m: f"('\'' || REPLACE({m.group(1)}, ',', '\',\'') || '\'')"),

    # ── Context variables ─────────────────────────────
    (re.compile(r'context\.getProperty\s*\(\s*[\'"](\w+)[\'"]\s*\)'),
     lambda m: f"'{{{{ var(\'{m.group(1)}\', \'\') }}}}'"),
    (re.compile(r'context\.(\w+)'),
     lambda m: f"'{{{{ var(\'{m.group(1)}\', \'\') }}}}'"),

    # ── globalMap (from 517 real jobs — cross-subjob state) ────
    # (String)globalMap.get("key") → dbt var
    (re.compile(r'\(\s*(?:String|Integer|Long|int|long|Object)\s*\)\s*globalMap\.get\s*\(\s*[\'"]([\w.]+)[\'"]\s*\)'),
     lambda m: f"'{{{{ var(\'{m.group(1)}\', \'\') }}}}'"),
    (re.compile(r'globalMap\.get\s*\(\s*[\'"]([\w.]+)[\'"]\s*\)'),
     lambda m: f"'{{{{ var(\'{m.group(1)}\', \'\') }}}}'"),
    (re.compile(r'globalMap\.containsKey\s*\(\s*[\'"]([\w.]+)[\'"]\s*\)'),
     lambda m: "TRUE"),

    # ── Reverse .equals: "literal".equals(expr) (seen in real filters) ────
    (re.compile(r'"([^"]*)"\s*\.equals\s*\(\s*(.+?)\s*\)'),
     lambda m: f"({m.group(2)} = '{m.group(1)}')"),

    # ── FieldHelper (TalendFramework custom routine, 509 jobs) ────
    (re.compile(r'FieldHelper\.isNotEmpty\s*\(\s*(.+?)\s*\)'),
     lambda m: f"({m.group(1)} IS NOT NULL AND {m.group(1)} != '')"),
    (re.compile(r'FieldHelper\.isEmpty\s*\(\s*(.+?)\s*\)'),
     lambda m: f"({m.group(1)} IS NULL OR {m.group(1)} = '')"),

    # ── Java type casts (common in tMap expressions) ────
    (re.compile(r'\(\s*(?:short|byte)\s*\)\s*(.+)'),
     lambda m: f"CAST({m.group(1)} AS SMALLINT)"),

    # ── Raw SQL dialect functions found in tMap expressions ────
    # MySQL DATE_FORMAT → DuckDB STRFTIME
    (re.compile(r'DATE_FORMAT\s*\(\s*(.+?)\s*,\s*([\'"][^\'"]+[\'"])\s*\)'),
     lambda m: f"STRFTIME({m.group(1)}, {m.group(2)})"),
    # MySQL IFNULL → DuckDB COALESCE
    (re.compile(r'IFNULL\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)'),
     lambda m: f"COALESCE({m.group(1)}, {m.group(2)})"),
    # MySQL NOW() → DuckDB CURRENT_TIMESTAMP
    (re.compile(r'NOW\s*\(\s*\)'),
     lambda m: "CURRENT_TIMESTAMP"),
    # MSSQL GETDATE() → DuckDB CURRENT_TIMESTAMP
    (re.compile(r'GETDATE\s*\(\s*\)'),
     lambda m: "CURRENT_TIMESTAMP"),
    # MSSQL CONVERT(type, expr, style) → DuckDB CAST
    (re.compile(r'CONVERT\s*\(\s*(\w+)\s*,\s*(.+?)\s*(?:,\s*\d+)?\s*\)'),
     lambda m: f"CAST({m.group(2)} AS {m.group(1)})"),
    # Oracle NVL → COALESCE
    (re.compile(r'NVL\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)'),
     lambda m: f"COALESCE({m.group(1)}, {m.group(2)})"),
    # Oracle SYSDATE → CURRENT_TIMESTAMP
    (re.compile(r'\bSYSDATE\b'),
     lambda m: "CURRENT_TIMESTAMP"),
]


def _apply_function_patterns(expr: str) -> tuple[str, bool]:
    """Apply all function-with-args patterns. Returns (result, was_changed)."""
    result = expr
    changed = False
    # Multiple passes since some patterns can be nested
    for _ in range(3):
        pass_changed = False
        for pattern, replacer in _FUNC_PATTERNS:
            new = pattern.sub(replacer, result)
            if new != result:
                result = new
                pass_changed = True
                changed = True
        if not pass_changed:
            break
    return result, changed


# ═══════════════════════════════════════════════════════════
# STAGE 4: Java method calls on objects
# e.g. row1.name.toUpperCase() → UPPER(row1.name)
# ═══════════════════════════════════════════════════════════

_METHOD_PATTERNS: list[tuple[re.Pattern, callable]] = [
    # .toUpperCase() → UPPER()
    (re.compile(r'(\w+(?:\.\w+)*)\.toUpperCase\s*\(\s*\)'),
     lambda m: f"UPPER({m.group(1)})"),
    # .toLowerCase() → LOWER()
    (re.compile(r'(\w+(?:\.\w+)*)\.toLowerCase\s*\(\s*\)'),
     lambda m: f"LOWER({m.group(1)})"),
    # .trim() → TRIM()
    (re.compile(r'(\w+(?:\.\w+)*)\.trim\s*\(\s*\)'),
     lambda m: f"TRIM({m.group(1)})"),
    # .length() → LENGTH()
    (re.compile(r'(\w+(?:\.\w+)*)\.length\s*\(\s*\)'),
     lambda m: f"LENGTH({m.group(1)})"),
    # .substring(start, end) → SUBSTRING(col, start+1, end-start)
    (re.compile(r'(\w+(?:\.\w+)*)\.substring\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)'),
     lambda m: f"SUBSTRING({m.group(1)}, {int(m.group(2))+1}, {int(m.group(3))-int(m.group(2))})"),
    # .substring(start) → SUBSTRING(col, start+1)
    (re.compile(r'(\w+(?:\.\w+)*)\.substring\s*\(\s*(\d+)\s*\)'),
     lambda m: f"SUBSTRING({m.group(1)}, {int(m.group(2))+1})"),
    # .indexOf("x") → STRPOS(col, 'x') - 1
    (re.compile(r'(\w+(?:\.\w+)*)\.indexOf\s*\(\s*"([^"]*)"\s*\)'),
     lambda m: f"(STRPOS({m.group(1)}, '{m.group(2)}') - 1)"),
    # .replace("a", "b") → REPLACE(col, 'a', 'b')
    (re.compile(r'(\w+(?:\.\w+)*)\.replace\s*\(\s*"([^"]*)"\s*,\s*"([^"]*)"\s*\)'),
     lambda m: f"REPLACE({m.group(1)}, '{m.group(2)}', '{m.group(3)}')"),
    # .replaceAll("regex", "repl") → REGEXP_REPLACE(col, regex, repl, 'g')
    (re.compile(r'(\w+(?:\.\w+)*)\.replaceAll\s*\(\s*"([^"]*)"\s*,\s*"([^"]*)"\s*\)'),
     lambda m: f"REGEXP_REPLACE({m.group(1)}, '{m.group(2)}', '{m.group(3)}', 'g')"),
    # .matches("regex") → REGEXP_MATCHES(col, 'regex')
    (re.compile(r'(\w+(?:\.\w+)*)\.matches\s*\(\s*"([^"]*)"\s*\)'),
     lambda m: f"REGEXP_MATCHES({m.group(1)}, '{m.group(2)}')"),
    # .startsWith("x") → STARTS_WITH(col, 'x')
    (re.compile(r'(\w+(?:\.\w+)*)\.startsWith\s*\(\s*"([^"]*)"\s*\)'),
     lambda m: f"STARTS_WITH({m.group(1)}, '{m.group(2)}')"),
    # .endsWith("x") → ENDS_WITH(col, 'x')
    (re.compile(r'(\w+(?:\.\w+)*)\.endsWith\s*\(\s*"([^"]*)"\s*\)'),
     lambda m: f"SUFFIX({m.group(1)}, '{m.group(2)}')"),
    # .contains("x") → CONTAINS(col, 'x')
    (re.compile(r'(\w+(?:\.\w+)*)\.contains\s*\(\s*"([^"]*)"\s*\)'),
     lambda m: f"CONTAINS({m.group(1)}, '{m.group(2)}')"),
    # .isEmpty() → (col IS NULL OR col = '')
    (re.compile(r'(\w+(?:\.\w+)*)\.isEmpty\s*\(\s*\)'),
     lambda m: f"({m.group(1)} IS NULL OR {m.group(1)} = '')"),
    # .equals("x") → col = 'x'
    (re.compile(r'(\w+(?:\.\w+)*)\.equals\s*\(\s*"([^"]*)"\s*\)'),
     lambda m: f"({m.group(1)} = '{m.group(2)}')"),
    # .equalsIgnoreCase("x") → LOWER(col) = LOWER('x')
    (re.compile(r'(\w+(?:\.\w+)*)\.equalsIgnoreCase\s*\(\s*"([^"]*)"\s*\)'),
     lambda m: f"(LOWER({m.group(1)}) = '{m.group(2).lower()}')"),
    # .toString() → CAST(col AS VARCHAR)
    (re.compile(r'(\w+(?:\.\w+)*)\.toString\s*\(\s*\)'),
     lambda m: f"CAST({m.group(1)} AS VARCHAR)"),
    # .intValue() → CAST(col AS INTEGER)
    (re.compile(r'(\w+(?:\.\w+)*)\.intValue\s*\(\s*\)'),
     lambda m: f"({m.group(1)})::INTEGER"),
    # NOTE: Integer.parseInt, Long.parseLong, Double.parseDouble, Float.parseFloat,
    # String.valueOf, Boolean.valueOf, Math.abs, Math.round are handled in Stage 4b
    # (_apply_wrapping_functions) using balanced paren matching for nested expressions.
    # Math.max(a, b) → GREATEST(a, b)  (two-arg, kept here)
    (re.compile(r'Math\.max\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)'),
     lambda m: f"GREATEST({m.group(1)}, {m.group(2)})"),
    # Math.min(a, b) → LEAST(a, b)  (two-arg, kept here)
    (re.compile(r'Math\.min\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)'),
     lambda m: f"LEAST({m.group(1)}, {m.group(2)})"),
]


def _find_balanced_paren(s: str, start: int) -> int:
    """Find the index of the closing ')' that matches the '(' at position start.
    Returns -1 if not found."""
    depth = 0
    for i in range(start, len(s)):
        if s[i] == '(':
            depth += 1
        elif s[i] == ')':
            depth -= 1
            if depth == 0:
                return i
    return -1


# Wrapping functions: Type.method(expr) where expr may contain nested parens
_WRAP_FUNCTIONS: list[tuple[re.Pattern, str]] = [
    (re.compile(r'Integer\.parseInt\s*\('), 'CAST({inner} AS INTEGER)'),
    (re.compile(r'Integer\.valueOf\s*\('),  'CAST({inner} AS INTEGER)'),
    (re.compile(r'Long\.parseLong\s*\('),   'CAST({inner} AS BIGINT)'),
    (re.compile(r'Double\.parseDouble\s*\('),'CAST({inner} AS DOUBLE)'),
    (re.compile(r'Float\.parseFloat\s*\('),  'CAST({inner} AS FLOAT)'),
    (re.compile(r'String\.valueOf\s*\('),    'CAST({inner} AS VARCHAR)'),
    (re.compile(r'Boolean\.valueOf\s*\('),   'CAST({inner} AS BOOLEAN)'),
    (re.compile(r'Math\.abs\s*\('),          'ABS({inner})'),
    (re.compile(r'Math\.round\s*\('),        'ROUND({inner})'),
]


def _apply_wrapping_functions(expr: str) -> tuple[str, bool]:
    """Handle Type.method(nested_expr) with balanced paren matching.
    Must run AFTER regex stages so inner expressions are already translated."""
    result = expr
    changed = False
    for _ in range(5):  # multiple passes for nested wraps
        pass_changed = False
        for pattern, template in _WRAP_FUNCTIONS:
            m = pattern.search(result)
            if m:
                open_idx = m.end() - 1  # position of '('
                close_idx = _find_balanced_paren(result, open_idx)
                if close_idx > open_idx:
                    inner = result[open_idx + 1:close_idx].strip()
                    replacement = template.format(inner=inner)
                    result = result[:m.start()] + replacement + result[close_idx + 1:]
                    pass_changed = True
                    changed = True
        if not pass_changed:
            break
    return result, changed


def _apply_method_patterns(expr: str) -> tuple[str, bool]:
    """Apply Java method call translations. Returns (result, was_changed)."""
    result = expr
    changed = False
    for _ in range(3):
        pass_changed = False
        for pattern, replacer in _METHOD_PATTERNS:
            new = pattern.sub(replacer, result)
            if new != result:
                result = new
                pass_changed = True
                changed = True
        if not pass_changed:
            break
    return result, changed


# ═══════════════════════════════════════════════════════════
# STAGE 5: Java operator normalization
# ═══════════════════════════════════════════════════════════

def _normalize_java_operators(expr: str) -> str:
    """Convert Java operators to SQL equivalents."""
    result = expr

    # null checks FIRST (before == normalization)
    result = re.sub(r'(\w+(?:\.\w+)*)\s*!=\s*null\b', r'\1 IS NOT NULL', result)
    result = re.sub(r'(\w+(?:\.\w+)*)\s*==\s*null\b', r'\1 IS NULL', result)
    result = re.sub(r'\bnull\s*!=\s*(\w+(?:\.\w+)*)', r'\1 IS NOT NULL', result)
    result = re.sub(r'\bnull\s*==\s*(\w+(?:\.\w+)*)', r'\1 IS NULL', result)

    # Java ternary → SQL CASE WHEN (handle non-nested only)
    # Must be done carefully to avoid mangling complex expressions
    ternary = re.match(r'^(.+?)\s*\?\s*(.+?)\s*:\s*(.+)$', result)
    if ternary:
        cond, then, else_ = ternary.group(1), ternary.group(2), ternary.group(3)
        # Only convert if cond doesn't contain unmatched parens suggesting nesting
        if cond.count('(') == cond.count(')') and '?' not in then:
            result = f"CASE WHEN {cond} THEN {then} ELSE {else_} END"

    # Boolean operators
    result = re.sub(r'\s*&&\s*', ' AND ', result)
    result = re.sub(r'\s*\|\|\s*', ' OR ', result)
    # ! prefix for negation (but not !=)
    result = re.sub(r'(?<!=)!(?!=)\s*(\w)', r'NOT \1', result)

    # == → = (AFTER null checks are handled)
    result = result.replace("==", "=")

    # Java string concatenation with + → ||
    # Pattern: expr + "string" or "string" + expr
    result = re.sub(r'"\s*\+\s*', "' || ", result)
    result = re.sub(r'\s*\+\s*"', " || '", result)

    # Standalone Java null literal → SQL NULL
    result = re.sub(r'\bnull\b', 'NULL', result)

    return result


# ═══════════════════════════════════════════════════════════
# STAGE 6: Java literal normalization
# ═══════════════════════════════════════════════════════════

def _normalize_literals(expr: str) -> str:
    """Convert Java string literals to SQL string literals."""
    # "string" → 'string' (but not inside function args that are already handled)
    # Only convert top-level double-quoted strings
    result = re.sub(r'"([^"]*)"', r"'\1'", expr)
    return result


# ═══════════════════════════════════════════════════════════
# DETECTION: Check if Java constructs remain after translation
# ═══════════════════════════════════════════════════════════

_JAVA_INDICATORS = [
    '.put(', '.set(', 'new ', 'System.', 'import ',
    'HashMap', 'ArrayList', 'Iterator', '.get(',
    '.class', 'throw ', 'try ', 'catch ', 'finally ',
    'for (', 'while (', 'switch ', 'return ',
    'BufferedReader', 'FileWriter', 'HttpClient',
    'java.sql.', 'java.util.', 'java.io.', 'java.net.',
    'org.', 'com.', 'javax.',
]


def _has_remaining_java(expr: str) -> bool:
    """Check if an expression still contains untranslatable Java constructs."""
    for indicator in _JAVA_INDICATORS:
        if indicator in expr:
            return True
    # Check for Java-style method chains not caught by patterns
    if re.search(r'\.\w+\s*\(', expr):
        # Exclude SQL functions we might have generated
        sql_funcs = {'UPPER(', 'LOWER(', 'TRIM(', 'LENGTH(', 'CAST(', 'COALESCE(',
                     'SUBSTRING(', 'REPLACE(', 'ABS(', 'ROUND(', 'STRPOS(', 'LEFT(',
                     'RIGHT(', 'STRFTIME(', 'STRPTIME(', 'DATE_DIFF(', 'DATE_TRUNC(',
                     'LAST_DAY(', 'TRY_STRPTIME(', 'REGEXP_REPLACE(', 'REGEXP_MATCHES(',
                     'CONCAT(', 'POWER(', 'SQRT(', 'CEIL(', 'FLOOR(', 'LN(', 'LOG10(',
                     'CHR(', 'PRINTF(', 'GREATEST(', 'LEAST(', 'LPAD(', 'RPAD(',
                     'STARTS_WITH(', 'SUFFIX(', 'CONTAINS(', 'REPEAT(', 'ROW_NUMBER(',
                     'EXTRACT(', 'INTERVAL', 'OVER ('}
        remaining = expr
        for sf in sql_funcs:
            remaining = remaining.replace(sf, '')
        if re.search(r'\.\w+\s*\(', remaining):
            return True
    return False


# ═══════════════════════════════════════════════════════════
# PUBLIC API: The 6-stage translation pipeline
# ═══════════════════════════════════════════════════════════

def translate_expression(expr: str) -> tuple[str, ExpressionStrategy]:
    """Translate a Talend Java expression to DuckDB SQL using the 6-stage pipeline.

    Returns (translated_sql, strategy) where strategy indicates how translation was done:
      - DETERMINISTIC: simple column ref, literal, arithmetic
      - KNOWLEDGE_BASE: translated via routine/pattern matching
      - LLM_REQUIRED: contains Java that we can't handle deterministically
    """
    if not expr or not expr.strip():
        return "NULL", ExpressionStrategy.DETERMINISTIC

    original = expr.strip()

    # Strip the Java package prefix: routines.TalendDate → TalendDate
    # Talend sometimes generates fully-qualified routine calls
    original = re.sub(r'\broutines\.(\w)', r'\1', original)

    # ── Fast paths ────────────────────────────────────
    # Pure numeric literal
    if original.replace('.', '', 1).replace('-', '', 1).isdigit():
        return original, ExpressionStrategy.DETERMINISTIC

    # Pure column reference: no spaces, no parens, just dots
    if re.match(r'^[\w]+\.[\w]+$', original) and not any(
        kw in original for kw in ('StringHandling', 'TalendDate', 'Numeric',
                                   'Relational', 'Mathematical', 'DataOperation',
                                   'context', 'globalMap')
    ):
        return original, ExpressionStrategy.DETERMINISTIC

    # ── Stage 1: System variables ─────────────────────
    result = _replace_system_vars(original)
    stage1_changed = (result != original)

    # ── Stage 2: Static zero-arg routines ─────────────
    result, stage2_changed = _replace_static_routines(result)

    # ── Stage 3: Function-with-args patterns ──────────
    result, stage3_changed = _apply_function_patterns(result)

    # ── Stage 4a: Java method calls (regex) ─────────
    result, stage4_changed = _apply_method_patterns(result)

    # ── Stage 4b: Wrapping functions (balanced parens) ──
    result, stage4b_changed = _apply_wrapping_functions(result)

    # ── Stage 5: Operator normalization ───────────────
    result = _normalize_java_operators(result)

    # ── Stage 6: Literal normalization ────────────────
    result = _normalize_literals(result)

    # ── Determine strategy ────────────────────────────
    any_kb = stage1_changed or stage2_changed or stage3_changed or stage4_changed or stage4b_changed

    if _has_remaining_java(result):
        return result, ExpressionStrategy.LLM_REQUIRED
    elif any_kb:
        return result, ExpressionStrategy.KNOWLEDGE_BASE
    else:
        return result, ExpressionStrategy.DETERMINISTIC
