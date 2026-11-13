"""SQLite connection helpers and schema management.

Two physical databases are used so that migration batches never take a write
transaction on the business (wide-table) database:

* old.db  - legacy wide table ``reports`` (business reads/writes while running)
* new.db  - long table ``results`` plus migration bookkeeping tables
"""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager

DATA_DIR = os.environ.get("DATA_DIR", "/data")
OLD_DB_PATH = os.path.join(DATA_DIR, "old.db")
NEW_DB_PATH = os.path.join(DATA_DIR, "new.db")

# Migration lifecycle states.
STATE_IDLE = "idle"            # never started
STATE_RUNNING = "running"      # batches being copied / reconciled
STATE_CUTOVER = "cutover"      # draining last changes under old-DB write lock
STATE_COMPLETED = "completed"  # all reads & writes go through the long table
STATE_FAILED = "failed"

# Key / value metadata keys persisted in new.db.
META_STATE = "state"
META_LAST_ID = "last_report_id"     # high-water mark of the batch scan
META_LAST_BATCH = "last_batch"      # last finished batch sequence number
META_TOTAL_ROWS = "total_rows"      # distinct report rows in results
META_LAST_ERROR = "last_error"
META_STARTED_AT = "started_at"
META_COMPLETED_AT = "completed_at"
META_SEED_TS = "seed_ts"

ITEMS = ("chromium", "lead")


def _connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    # WAL keeps readers working while another connection writes; the two
    # databases are separate files anyway, so migration writes never block
    # business writes on the old database.
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


@contextmanager
def old_conn():
    conn = _connect(OLD_DB_PATH)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def new_conn():
    conn = _connect(NEW_DB_PATH)
    try:
        yield conn
    finally:
        conn.close()


def init_dbs() -> None:
    """Create both databases and all schemas. Idempotent across restarts."""
    os.makedirs(DATA_DIR, exist_ok=True)
    with old_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS reports (
                report_id TEXT PRIMARY KEY,
                chromium  TEXT,
                lead      TEXT
            )
            """
        )

    with new_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS results (
                report_id TEXT NOT NULL,
                item      TEXT NOT NULL CHECK (item IN ('chromium','lead')),
                value     TEXT,
                PRIMARY KEY (report_id, item)
            )
            """
        )
        # Migration progress / lifecycle state.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS migration_meta (
                key   TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        # Per-row copy tracking: where the currently copied value came from.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS migration_seen (
                report_id TEXT PRIMARY KEY,
                chromium  TEXT,
                lead      TEXT
            )
            """
        )
        # Rows the application wrote directly to the long table during (or
        # after) cutover. The migrator must never overwrite these.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS locked_reports (
                report_id TEXT PRIMARY KEY
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS migration_errors (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_no   INTEGER,
                report_id  TEXT,
                message    TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        if get_meta(conn, META_STATE) is None:
            set_meta(conn, META_STATE, STATE_IDLE)


# --------------------------------------------------------------------------- #
# Metadata helpers
# --------------------------------------------------------------------------- #
def get_meta(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute(
        "SELECT value FROM migration_meta WHERE key = ?", (key,)
    ).fetchone()
    return row["value"] if row else None


def set_meta(conn: sqlite3.Connection, key: str, value) -> None:
    conn.execute(
        """
        INSERT INTO migration_meta(key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, None if value is None else str(value)),
    )


def get_state(conn: sqlite3.Connection) -> str:
    return get_meta(conn, META_STATE) or STATE_IDLE


def is_long_table_live(state: str) -> bool:
    """Whether application reads and writes must use the long table."""
    return state in (STATE_CUTOVER, STATE_COMPLETED)
