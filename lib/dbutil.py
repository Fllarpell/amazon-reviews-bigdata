"""PostgreSQL helpers: password file, connection, and simple SQL execution."""

from pathlib import Path

import psycopg2
from psycopg2.extensions import connection as PgConnection

from config.constants import PGDATABASE, PGHOST, PGPASSWORD_FILE, PGPORT, PGUSER


def read_password(path: Path) -> str:
    text = path.read_text(encoding="utf-8").strip().splitlines()
    if not text:
        raise ValueError("empty password file")
    return text[0].strip()


def connect(*, autocommit: bool = True) -> PgConnection:
    password = read_password(PGPASSWORD_FILE)
    conn = psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        user=PGUSER,
        password=password,
    )
    conn.autocommit = autocommit
    return conn


def exec_sql_file(cursor, path: Path) -> None:
    raw = path.read_text(encoding="utf-8")
    for stmt in raw.split(";"):
        stmt = stmt.strip()
        if stmt:
            cursor.execute(stmt)


def exec_sql_script(cursor, path: Path) -> None:
    raw = path.read_text(encoding="utf-8").strip()
    if raw:
        cursor.execute(raw)
