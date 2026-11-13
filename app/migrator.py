"""Background wide-table -> long-table migration engine.

Guarantees implemented here:

* Batch scan + conversion runs entirely against the *new* database for
  writes, so it never opens a write transaction on the business database.
* Each batch writes converted rows, per-row tracking and the progress
  high-water mark in one new.db transaction -> restart resumes without
  re-copying the whole database.
* Reconciliation passes re-scan the wide table so corrections submitted
  while migration is running are never missed.
* Cutover takes the old-database write lock before the final drain.  While
  that lock is held the wide table cannot change, so the final completeness
  check (every non-locked wide row present with identical values in the long
  table) and the flip to ``completed`` are committed in a single new.db
  transaction.  The business write path takes the same old-DB lock before
  choosing its table, so after that commit it can only write the long table.
* Rows written directly to the long table are registered in
  ``locked_reports`` and late copies can never overwrite them.
"""
from __future__ import annotations

import os
import sqlite3
import threading
import time
from datetime import datetime, timezone

from . import hooks
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


class _Incomplete(Exception):
    """The long table was not complete at the instant cutover wanted to flip."""


class Migrator:
    def __init__(self) -> None:
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self.pause_before_cutover: threading.Event | None = None

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
            if self.pause_before_cutover is not None:
                # Test hook: let a caller create the exact interleaving it
                # wants to regress while the engine is between "tail drain
                # done" and "cutover state published".
                self.pause_before_cutover.wait()
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
    @staticmethod
    def _wide_rows(conn: sqlite3.Connection) -> dict[str, tuple]:
        """Wide-table snapshot as ``{report_id: (chromium, lead)}``.

        Works with both ``sqlite3.Row`` rows and plain tuples (the cutover
        lock connection is not always configured with a row factory).
        """
        return {
            r[0]: (r[1], r[2])
            for r in conn.execute(
                "SELECT report_id, chromium, lead FROM reports"
            )
        }

    @staticmethod
    def _locked_all(conn: sqlite3.Connection) -> set[str]:
        return {
            r[0] for r in conn.execute("SELECT report_id FROM locked_reports")
        }

    @staticmethod
    def _diff_rows(
        conn: sqlite3.Connection,
        wide_rows: dict[str, tuple],
        locked: set[str],
    ) -> list[tuple[str, str | None, str | None]]:
        """Rows whose long-table copy is missing, short or out of date.

        Values written by the application (``locked_reports``) are
        authoritative on the long table and are always skipped.
        """
        long_map: dict[str, dict[str, str | None]] = {}
        for r in conn.execute("SELECT report_id, item, value FROM results"):
            long_map.setdefault(r[0], {})[r[1]] = r[2]
        diff = []
        for rid, (chromium, lead) in wide_rows.items():
            if rid in locked:
                continue
            got = long_map.get(rid)
            if (
                got is None
                or set(got) != set(ITEMS)
                or got.get("chromium") != chromium
                or got.get("lead") != lead
            ):
                diff.append((rid, chromium, lead))
        return diff

    def _reconcile(self) -> int:
        with old_conn() as old:
            wide_rows = self._wide_rows(old)
        return self._reconcile_rows(wide_rows)

    def _reconcile_rows(self, wide_rows: dict[str, tuple]) -> int:
        """Copy every non-locked wide row that differs from the long table.

        The diff is computed in a read transaction; the write transaction
        re-reads ``locked_reports`` so an application write that committed
        in between is never overwritten by a stale copy.
        """
        with new_conn() as conn:
            conn.execute("BEGIN")
            try:
                locked = self._locked_all(conn)
                stale = self._diff_rows(conn, wide_rows, locked)
            finally:
                conn.execute("COMMIT")

        if not stale:
            return 0

        applied = 0
        with new_conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                locked_now = self._locked_all(conn)
                for rid, chromium, lead in stale:
                    if rid in locked_now:
                        # A concurrent app write made the long row
                        # authoritative after our snapshot.
                        continue
                    self._write_one(conn, rid, chromium, lead)
                    applied += 1
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
        return applied

    # ------------------------------------------------------------------ #
    # Cutover: lock old writers, final drain, flip state
    # ------------------------------------------------------------------ #
    def _cutover(self) -> None:
        """Drain the last changes under the old-DB write lock and flip state.

        The old-DB RESERVED lock is held from before the final drain until
        the ``completed`` commit.  While it is held no business write can
        commit (the write path serializes on that same lock before choosing
        a table), so the completeness check below is evaluated against a
        frozen wide table and completion plus store switch are published in
        one new.db commit.

        On failure the lifecycle is rolled back to ``running`` (with the
        error recorded) so the web console can retry from the same progress;
        only truly unexpected exceptions bubble up to the fatal handler.
        """
        with new_conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            set_meta(conn, META_STATE, STATE_CUTOVER)
            conn.execute("COMMIT")
        hooks.trigger("cutover_state_flipped")

        # RESERVED lock on the business DB: new writers block (busy_timeout),
        # in-flight writers finish first, then we hold the lock.
        lock_conn = sqlite3.connect(
            f"file:{OLD_DB_PATH}", timeout=30, uri=True
        )
        try:
            lock_conn.execute("PRAGMA busy_timeout=30000")
            lock_conn.execute("BEGIN IMMEDIATE")
            hooks.trigger("cutover_old_db_locked")

            for _attempt in range(3):
                wide_rows = self._wide_rows(lock_conn)
                self._reconcile_rows(wide_rows)
                if self._complete_if_ready(lock_conn, wide_rows):
                    hooks.trigger("cutover_completed")
                    return
                # A row slipped past the drain (e.g. a writer outside the
                # application path).  The wide table cannot change while we
                # hold the lock, so another drain converges.
            raise _Incomplete(
                "long table still differs after repeated locked drains"
            )
        except (sqlite3.Error, _Incomplete) as exc:
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

    def _complete_if_ready(
        self, lock_conn: sqlite3.Connection, wide_rows: dict[str, tuple]
    ) -> bool:
        """Flip to ``completed`` after proving the long table is complete."""
        with new_conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                locked = self._locked_all(conn)
                if self._diff_rows(conn, wide_rows, locked):
                    conn.execute("ROLLBACK")
                    return False

                # Point of no return: the long table is complete *and* the
                # wide table frozen (we hold its write lock).  This single
                # commit is the switch for both readers (``completed``) and
                # writers (same state check under the old-DB lock).
                hooks.trigger("cutover_before_completed")
                set_meta(conn, META_STATE, STATE_COMPLETED)
                set_meta(conn, META_COMPLETED_AT, _utcnow())
                set_meta(conn, META_LAST_ERROR, "")
                set_meta(
                    conn,
                    META_TOTAL_ROWS,
                    conn.execute(
                        "SELECT COUNT(*) AS c FROM "
                        "(SELECT DISTINCT report_id FROM results)"
                    ).fetchone()["c"],
                )
                conn.execute("COMMIT")
                return True
            except Exception:
                conn.execute("ROLLBACK")
                raise


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
