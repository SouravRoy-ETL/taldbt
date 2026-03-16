"""
DuckDB Engine: local compute engine for schema registry, SQL validation,
source data loading, and migration parity checking.

Every DuckDB connection loads the flock extension for LLM-in-SQL.
Flock connects to Ollama's OpenAI-compatible endpoint at localhost:11434.
"""
from __future__ import annotations
import duckdb
from typing import Optional
from taldbt.models.ast_models import SourceInfo, ColumnSchema


def create_connection(db_path: str = ":memory:", read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with flock extension loaded.
    This is the ONLY way to create DuckDB connections in taldbt.
    Every connection gets flock + Ollama secret configured."""
    con = duckdb.connect(db_path, read_only=read_only)
    if read_only:
        _load_flock_only(con)
    else:
        _install_flock(con)
    return con


def _install_flock(con: duckdb.DuckDBPyConnection) -> bool:
    """Install, load flock, register active LLM provider (local or cloud)."""
    try:
        con.execute("INSTALL flock FROM community")
        con.execute("LOAD flock")
        # Use llm_provider to get the active endpoint
        try:
            from taldbt.llm.llm_provider import get_active_provider
            provider = get_active_provider()
            con.execute(f"""
                CREATE SECRET IF NOT EXISTS (
                    TYPE OPENAI,
                    API_KEY '{provider.api_key}',
                    BASE_URL '{provider.base_url}'
                )
            """)
            try:
                con.execute(f"CREATE MODEL IF NOT EXISTS('active_llm', '{provider.default_model}', 'openai')")
            except Exception:
                pass
        except ImportError:
            # Fallback if llm_provider not available
            con.execute("""
                CREATE SECRET IF NOT EXISTS (
                    TYPE OPENAI,
                    API_KEY 'ollama',
                    BASE_URL 'http://localhost:11434/v1'
                )
            """)
        return True
    except Exception:
        return False


def _load_flock_only(con: duckdb.DuckDBPyConnection) -> bool:
    """Load flock on read-only connections (already installed)."""
    try:
        con.execute("LOAD flock")
        return True
    except Exception:
        return False


def check_flock(con: duckdb.DuckDBPyConnection) -> bool:
    """Check if flock extension is loaded on this connection."""
    try:
        con.execute("SELECT 1 WHERE 1=0")  # no-op to test connection
        # Try a flock-specific command
        con.execute("GET MODELS")
        return True
    except Exception:
        return False


class DuckDBEngine:
    def __init__(self, db_path: str = ":memory:"):
        self.con = create_connection(db_path)
        self._flock_available = check_flock(self.con)
        self._init_registry()

    @property
    def has_flock(self) -> bool:
        return self._flock_available

    def _init_registry(self):
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS _source_registry (
                source_id VARCHAR PRIMARY KEY,
                source_type VARCHAR,
                database_name VARCHAR,
                table_name VARCHAR,
                file_path VARCHAR,
                column_count INTEGER
            )
        """)
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS _column_registry (
                source_id VARCHAR,
                column_name VARCHAR,
                column_type VARCHAR,
                nullable BOOLEAN,
                is_key BOOLEAN,
                ordinal INTEGER,
                PRIMARY KEY (source_id, column_name)
            )
        """)

    def register_source(self, source: SourceInfo):
        """Register a discovered source and create its mock table."""
        sid = source.source_id
        self.con.execute("""
            INSERT OR REPLACE INTO _source_registry VALUES (?, ?, ?, ?, ?, ?)
        """, [
            sid, source.source_type.value,
            source.connection.database if source.connection else "",
            source.connection.table if source.connection else "",
            source.file_path, len(source.columns),
        ])

        for col in source.columns:
            self.con.execute("""
                INSERT OR REPLACE INTO _column_registry VALUES (?, ?, ?, ?, ?, ?)
            """, [sid, col.name, col.sql_type, col.nullable, col.is_key, col.ordinal])

        self._create_mock_table(sid, source.columns)

    def _create_mock_table(self, table_name: str, columns: list[ColumnSchema]):
        if not columns:
            return
        safe_name = table_name.replace("-", "_").replace(".", "_")
        col_defs = ", ".join(f'"{c.name}" {c.sql_type}' for c in columns)
        try:
            self.con.execute(f'DROP TABLE IF EXISTS "{safe_name}"')
            self.con.execute(f'CREATE TABLE "{safe_name}" ({col_defs})')
        except Exception:
            pass

    def load_csv(self, file_path: str, table_name: str) -> int:
        safe = table_name.replace("-", "_").replace(".", "_")
        self.con.execute(f'CREATE OR REPLACE TABLE "{safe}" AS SELECT * FROM read_csv_auto(\'{file_path}\')')
        return self.con.execute(f'SELECT COUNT(*) FROM "{safe}"').fetchone()[0]

    def load_json(self, file_path: str, table_name: str) -> int:
        safe = table_name.replace("-", "_").replace(".", "_")
        self.con.execute(f'CREATE OR REPLACE TABLE "{safe}" AS SELECT * FROM read_json_auto(\'{file_path}\')')
        return self.con.execute(f'SELECT COUNT(*) FROM "{safe}"').fetchone()[0]

    def load_parquet(self, file_path: str, table_name: str) -> int:
        safe = table_name.replace("-", "_").replace(".", "_")
        self.con.execute(f'CREATE OR REPLACE TABLE "{safe}" AS SELECT * FROM read_parquet(\'{file_path}\')')
        return self.con.execute(f'SELECT COUNT(*) FROM "{safe}"').fetchone()[0]

    def validate_sql(self, sql: str) -> dict:
        clean = sql.replace("{{", "").replace("}}", "").replace("{%", "").replace("%}", "")
        clean = clean.replace("source(", "-- source(").replace("ref(", "-- ref(")
        clean = clean.replace("var(", "-- var(")
        try:
            self.con.execute(f"EXPLAIN {clean}")
            return {"valid": True, "error": None}
        except Exception as e:
            return {"valid": False, "error": str(e)}

    def get_registered_sources(self) -> list[dict]:
        rows = self.con.execute("SELECT * FROM _source_registry").fetchall()
        cols = ["source_id", "source_type", "database_name", "table_name", "file_path", "column_count"]
        return [dict(zip(cols, r)) for r in rows]

    def close(self):
        self.con.close()
