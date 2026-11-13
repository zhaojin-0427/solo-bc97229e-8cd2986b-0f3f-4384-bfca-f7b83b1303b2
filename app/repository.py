"""Business data access.

Routes report reads/writes to the legacy wide table while migration is in
progress and to the long table once cutover has committed ``completed``.
Values are nullable text: an empty form field is stored as NULL.

Race safety:

* **One lock for write routing.**  Every write takes the old-database
  ``BEGIN IMMEDIATE`` lock *before* reading the lifecycle state and writes
  its wide row in that same transaction.  Cutover holds that lock from
  before its final drain until the ``completed`` commit, so a business
  write is either fully committed and drained, or observes ``completed``
  and goes to the long table.  It can never commit to the frozen wide
  table afterwards.
* **No cross-table mirroring.**  A correction accepted while the wide table
  was authoritative is only written there; it is never replayed into the
  long table later, so a slower in-flight correction cannot overwrite a
  newer correction that committed after cutover.
* **Serialized long-table writes.**  Post-cutover writes take their own
  ``new.db`` write transaction, so concurrent corrections to the same report
  are applied atomically and cannot interleave into a half-written row.
"""
from __future__ import annotations

from . import hooks
from .db import (
    ITEMS,
    STORE_LONG,
    STORE_WIDE,
    get_state_snapshot,
    new_conn,
    old_conn,
    resolve_store,
)

# How many routing attempts before giving up on implausible contention.
_WRITE_RETRIES = 8


def _norm(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    return value or None


def _snapshot_state() -> str:
    with new_conn() as conn:
        conn.execute("BEGIN")
        try:
            return get_state_snapshot(conn)
        finally:
            conn.execute("COMMIT")


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
    # Read the wide copy first, *then* sample the state.  If the switch has
    # committed by then, the long table is known complete (cutover proved it
    # under the old-DB write lock), so reading it returns the newest bytes
    # and can never lose a report committed to the wide table earlier.  If
    # the sample still says ``cutover``, the wide read is authoritative.
    wide = _read_from_old(report_id)
    if resolve_store(_snapshot_state()) == STORE_LONG:
        return _read_from_new(report_id)
    return wide


def list_reports(limit: int = 100) -> list[dict]:
    """List reports from whichever store is authoritative for the state."""
    with old_conn() as conn:
        wide_rows = conn.execute(
            "SELECT report_id, chromium, lead FROM reports "
            "ORDER BY report_id LIMIT ?",
            (limit,),
        ).fetchall()
    if resolve_store(_snapshot_state()) != STORE_LONG:
        return [dict(r) for r in wide_rows]

    merged: dict[str, dict] = {}
    with new_conn() as conn:
        for r in conn.execute(
            "SELECT report_id, item, value FROM results ORDER BY report_id"
        ):
            entry = merged.setdefault(
                r["report_id"],
                {"report_id": r["report_id"],
                 "chromium": None, "lead": None},
            )
            entry[r["item"]] = r["value"]
    # Keep the listing total identical whether the sampled state was
    # ``cutover`` or ``completed`` (after completion every wide row is also
    # in the long table, so this merge is normally a no-op).
    for r in wide_rows:
        merged.setdefault(r["report_id"], dict(r))
    return sorted(merged.values(), key=lambda x: x["report_id"])[:limit]


# --------------------------------------------------------------------------- #
# Writes (insert / full-row correction)
# --------------------------------------------------------------------------- #
def upsert_report(report_id: str, chromium: str | None, lead: str | None) -> str:
    """Insert or fully correct one report row.

    Returns 'inserted' or 'updated'.  Commits independently of the migration
    so business writes are never blocked by batch processing; only the brief
    cutover hold of the old-DB lock can delay them.
    """
    report_id = (report_id or "").strip()
    if not report_id:
        raise ValueError("report_id is required")
    chromium = _norm(chromium)
    lead = _norm(lead)

    # Fast path: ``completed`` is terminal, so a write that observes it can
    # go straight to the long table without touching the old DB at all.
    if resolve_store(_snapshot_state()) == STORE_LONG:
        outcome = _upsert_long(report_id, chromium, lead)
        if outcome is not None:
            return outcome

    return _upsert_serialized(report_id, chromium, lead)


def _upsert_serialized(
    report_id: str, chromium: str | None, lead: str | None
) -> str:
    """Route a write under the old-DB write lock (shared with cutover)."""
    for _attempt in range(_WRITE_RETRIES):
        with old_conn() as conn:
            hooks.trigger("write_before_routing")
            # Serialize with cutover, which holds this lock from before its
            # final drain until the ``completed`` commit.  Taking it before
            # the state check and keeping it for the wide-table write means
            # a write can never land on the wide table after completion.
            conn.execute("BEGIN IMMEDIATE")
            try:
                state = _snapshot_state()
                hooks.trigger("write_holds_old_lock")
                if resolve_store(state) == STORE_WIDE:
                    existed = conn.execute(
                        "SELECT 1 FROM reports WHERE report_id = ?",
                        (report_id,),
                    ).fetchone()
                    conn.execute(
                        """
                        INSERT INTO reports(report_id, chromium, lead)
                        VALUES (?, ?, ?)
                        ON CONFLICT(report_id) DO UPDATE SET
                            chromium = excluded.chromium,
                            lead     = excluded.lead
                        """,
                        (report_id, chromium, lead),
                    )
                    # Release the old lock before opening new.db: cutover
                    # acquires old -> new, so the app must never hold old
                    # while waiting for new.
                    conn.execute("COMMIT")
                    # The correction is now durable in the wide table.  It
                    # is *not* mirrored into the long table: cutover's final
                    # drain (which serialized behind this commit) owns that
                    # copy, and any later long-table correction must win.
                    hooks.trigger("write_old_committed")
                    return "updated" if existed else "inserted"
                conn.execute("ROLLBACK")
            except Exception:
                conn.execute("ROLLBACK")
                raise

        # Completion committed before we could write the wide table: the
        # old table is frozen, so this correction goes to the long table.
        outcome = _upsert_long(report_id, chromium, lead)
        if outcome is not None:
            return outcome

    raise RuntimeError("report write kept losing races; retry later")


def _upsert_long(
    report_id: str, chromium: str | None, lead: str | None
) -> str | None:
    """Write the two long rows in one transaction, or None if re-routed."""
    with new_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            state = get_state_snapshot(conn)
            if resolve_store(state) != STORE_LONG:
                conn.execute("ROLLBACK")
                return None
            existed = conn.execute(
                "SELECT 1 FROM results WHERE report_id = ? LIMIT 1",
                (report_id,),
            ).fetchone()
            for item, value in zip(ITEMS, (chromium, lead)):
                conn.execute(
                    """
                    INSERT INTO results(report_id, item, value) VALUES (?, ?, ?)
                    ON CONFLICT(report_id, item)
                        DO UPDATE SET value = excluded.value
                    """,
                    (report_id, item, value),
                )
            # Late migration copies must never overwrite direct app writes.
            conn.execute(
                "INSERT OR IGNORE INTO locked_reports(report_id) VALUES (?)",
                (report_id,),
            )
            conn.execute("COMMIT")
            return "updated" if existed else "inserted"
        except Exception:
            conn.execute("ROLLBACK")
            raise
