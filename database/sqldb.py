import sqlite3
from pathlib import Path
from urllib.parse import urlparse

from info import SQLDB, TURSO_AUTH_TOKEN


def sqldb_enabled() -> bool:
    return bool(SQLDB)


def _is_remote_libsql(db_url: str) -> bool:
    return db_url.startswith("libsql://") or db_url.startswith("ws://") or db_url.startswith("wss://")


def _require_libsql_client() -> None:
    try:
        import libsql_client  # noqa: F401
    except Exception as e:
        raise RuntimeError(
            "Turso/libsql URL detected, but dependency 'libsql-client' is not installed. "
            "Please ensure requirements are installed (pip install -r requirements.txt)."
        ) from e


def _validate_remote_libsql(db_url: str) -> None:
    if not _is_remote_libsql(db_url):
        return

    if not TURSO_AUTH_TOKEN:
        raise RuntimeError(
            "Turso URL detected in SQLDB/TURSO_DATABASE_URL but TURSO_AUTH_TOKEN is missing. "
            "Add TURSO_AUTH_TOKEN in your environment variables."
        )

    _require_libsql_client()

    # NOTE:
    # Current DB modules are implemented with sqlite-style SQL execution.
    # Remote Turso/libsql needs a dedicated query adapter layer.
    # We keep this explicit failure to avoid silent misbehavior.
    raise RuntimeError(
        "Remote Turso URL detected and libsql-client is installed, but this code path still uses sqlite-style execution. "
        "Please use SQLDB=sqlite:///data/bot.db for now, or add a libsql query adapter in database modules."
    )


def get_sqldb_path() -> str:
    db_url = SQLDB.strip()
    if not db_url:
        return ""

    _validate_remote_libsql(db_url)

    if db_url.startswith("sqlite:///"):
        return db_url.replace("sqlite:///", "", 1)
    if db_url.startswith("sqlite://"):
        return db_url.replace("sqlite://", "", 1)

    parsed = urlparse(db_url)
    if parsed.scheme and parsed.scheme != "file":
        raise RuntimeError(
            f"Unsupported SQLDB scheme '{parsed.scheme}'. Use sqlite file path (e.g. sqlite:///data/bot.db)."
        )

    return db_url


def get_conn() -> sqlite3.Connection:
    db_path = get_sqldb_path() or "bot.db"
    path = Path(db_path)
    if path.parent and str(path.parent) != ".":
        path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn
