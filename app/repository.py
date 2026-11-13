"""Business data access.

Routes report reads/writes to the legacy wide table while migration is in
progress and to the long table once cutover/completed. Values are nullable
text: an empty form field is stored as NULL.
"""
from __future__ import annotations

import sqlite3

from .db import (
    ITEMS,
    get_state,
    is_long_table_live,
    new_conn,
    old_conn,
)


def _norm(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    return value or None


# --------------------------------------------------------------------------- #
# Reads
# --------------------------------------------------------------------------- #
def _read_from_old(report_id: str) -> dict | None:
    with old_conn() as conn:
        row = conn.execute(
            "SELECT report_id, chromium, lead FROM reports WHERE report_id = ?",
            (report_id,),
        ).fetchone()
    return dict(row) if row else None


def _read_from_new(report_id: str) -> dict | None:
    with new_conn() as conn:
        rows = conn.execute(
            "SELECT item, value FROM results WHERE report_id = ?", (report_id,)
        ).fetchall()
    if not rows:
        return None
    values = {r["item"]: r["value"] for r in rows}
    return {
        "report_id": report_id,
        "chromium": values.get("chromium"),
        "lead": values.get("lead"),
    }


def get_report(report_id: str) -> dict | None:
    with new_conn() as conn:
        state = get_state(conn)
    if is_long_table_live(state):
        return _read_from_new(report_id)
    return _read_from_old(report_id)


def list_reports(limit: int = 100) -> list[dict]:
    """List reports from whichever store is authoritative for the state.

    During running state the wide table is authoritative. During cutover the
    two stores overlap, so rows from both are merged with the long table
    winning (it holds the most recent copies plus direct cutover writes).
    """
    with new_conn() as conn:
        state = get_state(conn)

    if not is_long_table_live(state):
        with old_conn() as conn:
            rows = conn.execute(
                "SELECT report_id, chromium, lead FROM reports "
                "ORDER BY report_id LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    merged: dict[str, dict] = {}
    with old_conn() as conn:
        for r in conn.execute(
            "SELECT report_id, chromium, lead FROM reports ORDER BY report_id"
        ):
            merged[r["report_id"]] = dict(r)
    with new_conn() as conn:
        for r in conn.execute(
            "SELECT report_id, item, value FROM results ORDER BY report_id"
        ):
            entry = merged.setdefault(
                r["report_id"],
                {"report_id": r["report_id"], "chromium": None, "lead": None},
            )
            entry[r["item"]] = r["value"]
    return sorted(merged.values(), key=lambda x: x["report_id"])[:limit]


# --------------------------------------------------------------------------- #
# Writes (insert / full-row correction)
# --------------------------------------------------------------------------- #
def upsert_report(report_id: str, chromium: str | None, lead: str | None) -> str:
    """Insert or fully correct one report row.

    Returns 'inserted' or 'updated'. Commits independently of the migration
    so business writes are never blocked by batch processing.
    """
    report_id = (report_id or "").strip()
    if not report_id:
        raise ValueError("report_id is required")
    chromium = _norm(chromium)
    lead = _norm(lead)

    with new_conn() as conn:
        state = get_state(conn)

    if is_long_table_live(state):
        return _upsert_long(report_id, chromium, lead)
    return _upsert_wide(report_id, chromium, lead)


def _upsert_wide(report_id: str, chromium: str | None, lead: str | None) -> str:
    with old_conn() as conn:
        existed = conn.execute(
            "SELECT 1 FROM reports WHERE report_id = ?", (report_id,)
        ).fetchone()
        conn.execute(
            """
            INSERT INTO reports(report_id, chromium, lead) VALUES (?, ?, ?)
            ON CONFLICT(report_id) DO UPDATE SET
                chromium = excluded.chromium,
                lead     = excluded.lead
            """,
            (report_id, chromium, lead),
        )
    # Re-check: if cutover happened in the meantime the migrator's drain may
    # miss this write; mirror it into the long table as well and lock it.
    with new_conn() as conn:
        state_now = get_state(conn)
        if is_long_table_live(state_now):
            _long_insert(conn, report_id, chromium, lead, lock=True)
    return "updated" if existed else "inserted"


def _upsert_long(report_id: str, chromium: str | None, lead: str | None) -> str:
    with new_conn() as conn:
        existed = conn.execute(
            "SELECT 1 FROM results WHERE report_id = ? LIMIT 1", (report_id,)
        ).fetchone()
        _long_insert(conn, report_id, chromium, lead, lock=True)
    return "updated" if existed else "inserted"


def _long_insert(
    conn: sqlite3.Connection,
    report_id: str,
    chromium: str | None,
    lead: str | None,
    lock: bool,
) -> None:
    """Write the two long-table rows (NULLs kept) in one transaction."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        for item, value in zip(ITEMS, (chromium, lead)):
            conn.execute(
                """
                INSERT INTO results(report_id, item, value) VALUES (?, ?, ?)
                ON CONFLICT(report_id, item)
                    DO UPDATE SET value = excluded.value
                """,
                (report_id, item, value),
            )
        if lock:
            conn.execute(
                "INSERT OR IGNORE INTO locked_reports(report_id) VALUES (?)",
                (report_id,),
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
