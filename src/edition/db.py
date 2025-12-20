from __future__ import annotations

import sqlite3
from pathlib import Path


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON;")
    return con


def init_db(db_path: Path, schema_path: Path) -> None:
    schema_sql = schema_path.read_text(encoding="utf-8")
    with connect(db_path) as con:
        con.executescript(schema_sql)
