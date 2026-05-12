"""Versioned SQL migrations with a ledger table in schema ``pipeline``."""

import logging
from pathlib import Path
from typing import List, Set

from psycopg2.extensions import connection as PgConnection

from config.constants import MIGRATIONS_DIR, PIPELINE_SCHEMA, SCHEMA_MIGRATIONS_TABLE
from lib.dbutil import exec_sql_file, exec_sql_script

logger = logging.getLogger(__name__)


def _ledger_table_sql() -> str:
    fq = f'"{PIPELINE_SCHEMA}"."{SCHEMA_MIGRATIONS_TABLE}"'
    return f"""
CREATE SCHEMA IF NOT EXISTS "{PIPELINE_SCHEMA}";
CREATE TABLE IF NOT EXISTS {fq} (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


def _ensure_ledger(cursor) -> None:
    raw = _ledger_table_sql()
    for stmt in raw.split(";"):
        stmt = stmt.strip()
        if stmt:
            cursor.execute(stmt)


def _applied_versions(cursor) -> Set[str]:
    fq = f'"{PIPELINE_SCHEMA}"."{SCHEMA_MIGRATIONS_TABLE}"'
    cursor.execute(f"SELECT version FROM {fq}")
    return {row[0] for row in cursor.fetchall()}


def applied_versions_ordered(cursor) -> List[str]:
    fq = f'"{PIPELINE_SCHEMA}"."{SCHEMA_MIGRATIONS_TABLE}"'
    cursor.execute(f"SELECT version FROM {fq} ORDER BY version")
    return [row[0] for row in cursor.fetchall()]


def _version_directories() -> List[Path]:
    if not MIGRATIONS_DIR.is_dir():
        raise FileNotFoundError(MIGRATIONS_DIR)
    result: List[Path] = []
    for path in sorted(MIGRATIONS_DIR.iterdir()):
        if not path.is_dir() or path.name.startswith((".", "_")):
            continue
        if (path / "deploy").is_dir() or (path / "deploy.sql").is_file():
            result.append(path)
    return result


def _deploy_sql_files(version_dir: Path) -> List[Path]:
    deploy_dir = version_dir / "deploy"
    if deploy_dir.is_dir():
        files = sorted(deploy_dir.glob("*.sql"))
        if not files:
            raise FileNotFoundError(deploy_dir)
        return files
    single = version_dir / "deploy.sql"
    if single.is_file():
        return [single]
    raise FileNotFoundError(version_dir / "deploy")


def apply_pending_migrations(conn: PgConnection) -> None:
    fq = f'"{PIPELINE_SCHEMA}"."{SCHEMA_MIGRATIONS_TABLE}"'
    with conn.cursor() as cur:
        _ensure_ledger(cur)
        done = _applied_versions(cur)
        for version_dir in _version_directories():
            version_id = version_dir.name
            if version_id in done:
                logger.info("migration %s already applied", version_id)
                continue
            logger.info("applying migration %s", version_id)
            for sql_path in _deploy_sql_files(version_dir):
                logger.info("  %s", sql_path.name)
                exec_sql_file(cur, sql_path)
            cur.execute(f"INSERT INTO {fq} (version) VALUES (%s)", (version_id,))


def run_migration_verify(cursor, version_id: str) -> None:
    path = MIGRATIONS_DIR / version_id / "verify.sql"
    if not path.is_file():
        raise FileNotFoundError(path)
    exec_sql_script(cursor, path)


def verify_applied_migrations(cursor) -> None:
    for version_id in applied_versions_ordered(cursor):
        logger.info("verify migration %s", version_id)
        run_migration_verify(cursor, version_id)


def revert_last_migration(conn: PgConnection) -> str:
    fq = f'"{PIPELINE_SCHEMA}"."{SCHEMA_MIGRATIONS_TABLE}"'
    with conn.cursor() as cur:
        applied = applied_versions_ordered(cur)
        if not applied:
            raise ValueError("no applied migrations")
        version_id = applied[-1]
        revert_path = MIGRATIONS_DIR / version_id / "revert.sql"
        if not revert_path.is_file():
            raise FileNotFoundError(revert_path)
        logger.info("revert migration %s", version_id)
        exec_sql_file(cur, revert_path)
        cur.execute(f"DELETE FROM {fq} WHERE version = %s", (version_id,))
    return version_id
