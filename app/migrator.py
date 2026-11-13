"""Background wide-table -> long-table migration engine.

Guarantees implemented here:

* Batch scan + conversion runs entirely against the *new* database for
  writes, so it never opens a write transaction on the business database.
* Each batch writes converted rows, per-row tracking and the progress
  high-water mark in one new.db transaction -> restart resumes without
  re-copying the whole database.
* Reconciliation passes re-scan the wide table so corrections submitted
  while migration is running are never missed.
* Cutover takes the old-database write lock before the final drain, then
  flips state to ``completed`` atomically; rows written directly to the
  long table are registered in ``locked_reports`` and late copies can
  never overwrite them.
"""
from __future__ import annotations

import os
import sqlite3
import threading
import time
from datetime import datetime, timezone

from .db import (
    META_COMPLETED_AT,
    META_LAST_BATCH,
    META_LAST_ERROR,
    META_LAST_ID,
    META_STARTED_AT,
    META_TOTAL_ROWS,
    META_STATE,
    STATE_COMPLETED,
    STATE_CUTOVER,
    STATE_FAILED,
    STATE_IDLE,
    STATE_RUNNING,
    ITEMS,
    OLD_DB_PATH,
    get_meta,
    get_state,
    new_conn,
    old_conn,
    set_meta,
)
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "200"))
# Small pacing delay so progress is observable in the browser and the
# engine yields between batches.
BATCH_DELAY = float(os.environ.get("BATCH_DELAY", "0.25"))


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Migrator:
    def __init__(self) -> None:
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    def start(self) -> bool:
        """Start (or resume) migration. Returns False if already finished."""
        with self._lock:
            with new_conn() as conn:
                state = get_state(conn)
                if state in (STATE_RUNNING, STATE_CUTOVER):
                    if self._thread and self._thread.is_alive():
                        return True
                    # Resume after a process restart: progress metadata is
                    # already on disk and must not be re-initialised.
                elif state in (STATE_IDLE, STATE_FAILED):
                    conn.execute("BEGIN IMMEDIATE")
                    set_meta(conn, META_STATE, STATE_RUNNING)
                    if get_meta(conn, META_STARTED_AT) is None:
                        # Genuine first start; a resumed/retrying run keeps
                        # its existing batch number and water mark.
                        set_meta(conn, META_STARTED_AT, _utcnow())
                        set_meta(conn, META_LAST_BATCH, 0)
                    conn.execute("COMMIT")
                else:
                    return False

            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run_safely, name="migrator", daemon=True
            )
            self._thread.start()
            return True

    def resume_if_needed(self) -> None:
        """Called once at app startup: continue an interrupted migration."""
        with new_conn() as conn:
            state = get_state(conn)
        if state in (STATE_RUNNING, STATE_CUTOVER):
            self.start()

    def stop(self) -> None:
        self._stop.set()

    def _run_safely(self) -> None:
        try:
            self._run()
        except Exception as exc:  # pragma: no cover - defensive
            with new_conn() as conn:
                conn.execute("BEGIN IMMEDIATE")
                set_meta(conn, META_STATE, STATE_FAILED)
                set_meta(conn, META_LAST_ERROR, f"fatal: {exc!r}")
                conn.execute("COMMIT")

    # ------------------------------------------------------------------ #
    # Main loop
    # ------------------------------------------------------------------ #
    def _run(self) -> None:
        while not self._stop.is_set():
            with new_conn() as conn:
                state = get_state(conn)
            if state == STATE_COMPLETED:
                return
            with new_conn() as conn:
                last_id = get_meta(conn, META_LAST_ID)
                batch_no = int(get_meta(conn, META_LAST_BATCH) or 0)

            rows = self._scan_batch(last_id)
            if rows:
                self._apply_batch(rows, batch_no + 1)
                time.sleep(BATCH_DELAY)
                continue

            # High-water scan reached the tail. One full reconciliation
            # drains corrections (including ids before the water mark);
            # any correction landing afterwards is caught by the *locked*
            # drain inside _cutover(), so we enter cutover immediately
            # instead of waiting for a quiet period that may never come
            # under a continuous correction workload.
            self._reconcile()

            if self._stop.is_set():
                self._mark_stopped()
                return
            self._cutover()
            if self._stop.is_set():
                # Cutover failed and rolled the state back to running;
                # honour a pending stop instead of retrying immediately.
                self._mark_stopped()
                return
            # On success the state is completed and the next loop exits;
            # on sqlite3 failure the state is running, so the loop retries.
            time.sleep(BATCH_DELAY)

        # Stopped mid-batch: roll the lifecycle back to idle so the UI can
        # resume; persisted progress (water mark + batches) stays intact.
        self._mark_stopped()

    def _mark_stopped(self) -> None:
        with new_conn() as conn:
            if get_state(conn) in (STATE_RUNNING, STATE_CUTOVER):
                conn.execute("BEGIN IMMEDIATE")
                set_meta(conn, META_STATE, STATE_IDLE)
                conn.execute("COMMIT")

    def _scan_batch(self, last_id: str | None) -> list[sqlite3.Row]:
        sql = (
            "SELECT report_id, chromium, lead FROM reports "
            "WHERE report_id > ? ORDER BY report_id LIMIT ?"
            if last_id is not None
            else "SELECT report_id, chromium, lead FROM reports "
            "ORDER BY report_id LIMIT ?"
        )
        params = (last_id, BATCH_SIZE) if last_id is not None else (BATCH_SIZE,)
        with old_conn() as conn:
            return conn.execute(sql, params).fetchall()

    # ------------------------------------------------------------------ #
    # Conversion
    # ------------------------------------------------------------------ #
    @staticmethod
    def _locked_ids(conn: sqlite3.Connection, ids: list[str]) -> set[str]:
        if not ids:
            return set()
        placeholders = ",".join("?" * len(ids))
        return {
            r["report_id"]
            for r in conn.execute(
                f"SELECT report_id FROM locked_reports "
                f"WHERE report_id IN ({placeholders})",
                ids,
            )
        }

    def _apply_batch(self, rows: list[sqlite3.Row], batch_no: int) -> None:
        """Convert one batch; rows + progress commit atomically in new.db."""
        converted, failed = [], []
        for row in rows:
            try:
                converted.append(
                    (row["report_id"], row["chromium"], row["lead"])
                )
            except Exception as exc:  # pragma: no cover - defensive
                failed.append((row["report_id"], repr(exc)))

        with new_conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                locked = self._locked_ids(
                    conn, [r[0] for r in converted]
                )
                applied = 0
                max_id = get_meta(conn, META_LAST_ID)
                for report_id, chromium, lead in converted:
                    if report_id in locked:
                        # Long-table value is newer; the copy is late.
                        continue
                    self._write_one(conn, report_id, chromium, lead)
                    applied += 1
                    if max_id is None or report_id > max_id:
                        max_id = report_id

                for report_id, message in failed:
                    conn.execute(
                        "INSERT INTO migration_errors"
                        "(batch_no, report_id, message) VALUES (?, ?, ?)",
                        (batch_no, report_id, message),
                    )
                    set_meta(conn, META_LAST_ERROR, message)

                set_meta(conn, META_LAST_BATCH, batch_no)
                if max_id is not None:
                    set_meta(conn, META_LAST_ID, max_id)
                set_meta(
                    conn,
                    META_TOTAL_ROWS,
                    conn.execute(
                        "SELECT COUNT(*) AS c FROM "
                        "(SELECT DISTINCT report_id FROM results)"
                    ).fetchone()["c"],
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    @staticmethod
    def _write_one(
        conn: sqlite3.Connection,
        report_id: str,
        chromium: str | None,
        lead: str | None,
    ) -> None:
        """Write the two long rows (NULL preserved) and update tracking."""
        for item, value in zip(ITEMS, (chromium, lead)):
            conn.execute(
                """
                INSERT INTO results(report_id, item, value) VALUES (?, ?, ?)
                ON CONFLICT(report_id, item)
                    DO UPDATE SET value = excluded.value
                """,
                (report_id, item, value),
            )
        conn.execute(
            """
            INSERT INTO migration_seen(report_id, chromium, lead)
            VALUES (?, ?, ?)
            ON CONFLICT(report_id) DO UPDATE SET
                chromium = excluded.chromium,
                lead     = excluded.lead
            """,
            (report_id, chromium, lead),
        )

    # ------------------------------------------------------------------ #
    # Reconciliation: re-scan everything, re-copy changed/missing rows
    # ------------------------------------------------------------------ #
    def _reconcile(self) -> int:
        with old_conn() as old:
            wide_rows = old.execute(
                "SELECT report_id, chromium, lead FROM reports"
            ).fetchall()
        with new_conn() as conn:
            seen = {
                r["report_id"]: r
                for r in conn.execute(
                    "SELECT report_id, chromium, lead FROM migration_seen"
                )
            }
            locked = {
                r["report_id"]
                for r in conn.execute("SELECT report_id FROM locked_reports")
            }

            stale: list[tuple[str, str | None, str | None]] = []
            for row in wide_rows:
                rid = row["report_id"]
                if rid in locked:
                    continue
                prev = seen.get(rid)
                if (
                    prev is None
                    or prev["chromium"] != row["chromium"]
                    or prev["lead"] != row["lead"]
                ):
                    stale.append((rid, row["chromium"], row["lead"]))

            if not stale:
                return 0

            conn.execute("BEGIN IMMEDIATE")
            try:
                for rid, chromium, lead in stale:
                    self._write_one(conn, rid, chromium, lead)
                set_meta(
                    conn,
                    META_TOTAL_ROWS,
                    conn.execute(
                        "SELECT COUNT(*) AS c FROM "
                        "(SELECT DISTINCT report_id FROM results)"
                    ).fetchone()["c"],
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return len(stale)

    # ------------------------------------------------------------------ #
    # Cutover: lock old writers, final drain, flip state
    # ------------------------------------------------------------------ #
    def _cutover(self) -> None:
        """Drain the last changes under the old-DB write lock and flip state.

        On failure the lifecycle is rolled back to ``running`` (with the
        error recorded) so the web console can retry from the same progress;
        only truly unexpected exceptions bubble up to the fatal handler.
        """
        with new_conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            set_meta(conn, META_STATE, STATE_CUTOVER)
            conn.execute("COMMIT")

        # RESERVED lock on the business DB: new writers block (busy_timeout),
        # in-flight writers finish first, then we hold the lock.
        lock_conn = sqlite3.connect(
            f"file:{OLD_DB_PATH}", timeout=30, uri=True
        )
        try:
            lock_conn.execute("PRAGMA busy_timeout=30000")
            lock_conn.execute("BEGIN IMMEDIATE")

            # Anything committed up to this instant is drained, and rows
            # mirrored by app writers during cutover are locked/skipped.
            self._reconcile()

            with new_conn() as conn:
                conn.execute("BEGIN IMMEDIATE")
                set_meta(conn, META_STATE, STATE_COMPLETED)
                set_meta(conn, META_COMPLETED_AT, _utcnow())
                set_meta(conn, META_LAST_ERROR, "")
                conn.execute("COMMIT")
        except sqlite3.Error as exc:
            with new_conn() as conn:
                conn.execute("BEGIN IMMEDIATE")
                set_meta(conn, META_STATE, STATE_RUNNING)
                set_meta(conn, META_LAST_ERROR, f"cutover retry: {exc!r}")
                conn.execute(
                    "INSERT INTO migration_errors"
                    "(batch_no, report_id, message) VALUES (NULL, NULL, ?)",
                    (f"cutover failed, will retry: {exc!r}",),
                )
                conn.execute("COMMIT")
        finally:
            # BEGIN IMMEDIATE itself may have failed (no active transaction).
            try:
                lock_conn.rollback()
            except sqlite3.Error:
                pass
            lock_conn.close()


# --------------------------------------------------------------------------- #
# Progress snapshots for the web UI
# --------------------------------------------------------------------------- #
def progress() -> dict:
    with old_conn() as conn:
        wide_total = conn.execute(
            "SELECT COUNT(*) AS c FROM reports"
        ).fetchone()["c"]
    with new_conn() as conn:
        long_total = conn.execute(
            "SELECT COUNT(DISTINCT report_id) AS c FROM results"
        ).fetchone()["c"]
        errors = conn.execute(
            "SELECT COUNT(*) AS c FROM migration_errors"
        ).fetchone()["c"]
        recent_errors = [
            dict(r)
            for r in conn.execute(
                "SELECT id, batch_no, report_id, message, created_at "
                "FROM migration_errors ORDER BY id DESC LIMIT 10"
            )
        ]
        meta = {
            r["key"]: r["value"]
            for r in conn.execute("SELECT key, value FROM migration_meta")
        }
    pct = round(100.0 * long_total / wide_total, 1) if wide_total else 100.0
    return {
        "state": meta.get(META_STATE, STATE_IDLE),
        "wide_total": wide_total,
        "long_total": long_total,
        "percent": min(pct, 100.0),
        "batch_no": int(meta.get(META_LAST_BATCH) or 0),
        "last_id": meta.get(META_LAST_ID),
        "last_error": meta.get(META_LAST_ERROR) or "",
        "started_at": meta.get(META_STARTED_AT),
        "completed_at": meta.get(META_COMPLETED_AT),
        "error_count": errors,
        "recent_errors": recent_errors,
    }


migrator = Migrator()
