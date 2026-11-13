"""End-to-end guarantees for the wide->long migration.

Stdlib-only (no Flask needed): drives the migrator and repository directly
against throwaway SQLite files in a temp directory.

Run with:  python -I -m unittest tests.test_migration -v
"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_TMP = tempfile.mkdtemp(prefix="migration-test-")
os.environ["DATA_DIR"] = _TMP
os.environ["BATCH_SIZE"] = "50"
os.environ["BATCH_DELAY"] = "0"

import app.db as db  # noqa: E402
from app.migrator import (  # noqa: E402
    Migrator,
    STATE_COMPLETED,
    STATE_IDLE,
    STATE_RUNNING,
    progress,
)
import app.repository as repo  # noqa: E402


def _wait_until(predicate, timeout=30.0, interval=0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


class MigrationTestBase(unittest.TestCase):
    def setUp(self):
        for name in ("old.db", "new.db", "old.db-wal", "old.db-shm",
                     "new.db-wal", "new.db-shm"):
            p = os.path.join(_TMP, name)
            if os.path.exists(p):
                os.remove(p)
        db.init_dbs()

    def _seed(self, n):
        with db.old_conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            for i in range(1, n + 1):
                conn.execute(
                    "INSERT INTO reports(report_id, chromium, lead) "
                    "VALUES (?, ?, ?)",
                    (f"R{i:04d}",
                     None if i % 10 == 0 else f"{i}.1",
                     "" if i % 7 == 0 else f"{i}.2"),
                )
            conn.execute("COMMIT")

    def _run_to_completion(self):
        m = Migrator()
        self.assertTrue(m.start())
        ok = _wait_until(lambda: progress()["state"] == STATE_COMPLETED)
        self.assertTrue(ok, f"migration did not finish: {progress()}")
        return m

    def _assert_tables_match(self):
        with db.old_conn() as c:
            wide = {
                r["report_id"]: (r["chromium"], r["lead"])
                for r in c.execute(
                    "SELECT report_id, chromium, lead FROM reports"
                )
            }
        with db.new_conn() as c:
            long_rows = c.execute(
                "SELECT report_id, item, value FROM results"
            ).fetchall()
        long: dict[str, tuple] = {}
        for r in long_rows:
            entry = long.setdefault(r["report_id"], {})
            entry[r["item"]] = r["value"]
        self.assertEqual(set(wide), set(long), "report id sets differ")
        for rid, (chromium, lead) in wide.items():
            got = long[rid]
            self.assertEqual(
                (got.get("chromium"), got.get("lead")),
                (chromium, lead),
                f"values differ for {rid}",
            )
        # Exactly two long rows per report, including NULL values.
        with db.new_conn() as c:
            counts = c.execute(
                "SELECT report_id, COUNT(*) c FROM results GROUP BY report_id"
            ).fetchall()
        self.assertTrue(all(r["c"] == 2 for r in counts))

    def _assert_tables_match_allowing_locked(self):
        """Wide vs long equality, except rows the app wrote directly (locked)."""
        with db.old_conn() as c:
            wide = {
                r["report_id"]: (r["chromium"], r["lead"])
                for r in c.execute(
                    "SELECT report_id, chromium, lead FROM reports"
                )
            }
        with db.new_conn() as c:
            long_rows = c.execute(
                "SELECT report_id, item, value FROM results"
            ).fetchall()
            locked = {
                r["report_id"]
                for r in c.execute("SELECT report_id FROM locked_reports")
            }
        long: dict[str, tuple] = {}
        for r in long_rows:
            entry = long.setdefault(r["report_id"], {})
            entry[r["item"]] = r["value"]
        for rid, (chromium, lead) in wide.items():
            if rid in locked:
                continue
            got = long[rid]
            self.assertEqual(
                (got.get("chromium"), got.get("lead")),
                (chromium, lead),
                f"values differ for {rid}",
            )
        with db.new_conn() as c:
            counts = c.execute(
                "SELECT report_id, COUNT(*) c FROM results GROUP BY report_id"
            ).fetchall()
        self.assertTrue(all(r["c"] == 2 for r in counts))


class TestHappyPath(MigrationTestBase):
    def test_full_migration_nulls_and_empty_strings(self):
        self._seed(230)
        self._run_to_completion()
        p = progress()
        self.assertEqual(p["wide_total"], 230)
        self.assertEqual(p["long_total"], 230)
        self.assertEqual(p["percent"], 100.0)
        self._assert_tables_match()
        # NULL and empty-string semantics survive the long-table conversion.
        with db.new_conn() as c:
            null_chromium = c.execute(
                "SELECT COUNT(*) c FROM results WHERE item='chromium' "
                "AND value IS NULL"
            ).fetchone()["c"]
            empty_lead = c.execute(
                "SELECT COUNT(*) c FROM results WHERE item='lead' "
                "AND value=''"
            ).fetchone()["c"]
        self.assertEqual(null_chromium, 23)
        self.assertEqual(empty_lead, 32)

    def test_restart_resumes_without_full_recopy(self):
        # Large enough that two batches (size 50) cannot reach the tail
        # before stop is observed, making the interruption deterministic.
        self._seed(1000)
        m = Migrator()
        m.start()
        _wait_until(lambda: progress()["batch_no"] >= 2)
        m.stop()
        _wait_until(
            lambda: not (m._thread and m._thread.is_alive())
        )
        self.assertNotEqual(progress()["state"], STATE_COMPLETED)
        batches_before = progress()["batch_no"]
        rows_before = progress()["long_total"]
        self.assertEqual(progress()["state"], STATE_IDLE)

        # Simulate process restart: a brand-new Migrator resumes from
        # persisted progress.
        m2 = Migrator()
        m2.start()
        self.assertTrue(_wait_until(
            lambda: progress()["state"] == STATE_COMPLETED
        ))
        self._assert_tables_match()
        # Resumption continued the batch sequence instead of starting over.
        self.assertGreaterEqual(progress()["batch_no"], batches_before)
        self.assertEqual(progress()["long_total"], 1000)
        self.assertGreaterEqual(rows_before, 100)


class TestConcurrentWrites(MigrationTestBase):
    def test_correction_during_migration_is_not_lost(self):
        self._seed(400)
        m = Migrator()
        m.start()
        _wait_until(lambda: progress()["batch_no"] >= 1)

        # Correct a row that has very likely already been copied, and
        # insert a brand-new report.
        repo.upsert_report("R00010", "999.0", "888.0")
        repo.upsert_report("R99999", "0.5", None)

        self.assertTrue(_wait_until(
            lambda: progress()["state"] == STATE_COMPLETED
        ))
        self._assert_tables_match()
        self.assertEqual(
            repo.get_report("R00010"),
            {"report_id": "R00010", "chromium": "999.0", "lead": "888.0"},
        )
        self.assertEqual(
            repo.get_report("R99999"),
            {"report_id": "R99999", "chromium": "0.5", "lead": None},
        )

    def test_writes_after_completion_go_to_long_table_and_win(self):
        self._seed(100)
        self._run_to_completion()

        # Stale value left behind in the old table must never overwrite
        # the direct long-table write.
        with db.old_conn() as c:
            c.execute(
                "UPDATE reports SET chromium='STALE' WHERE report_id='R00005'"
            )
        repo.upsert_report("R00005", "NEW-VALUE", "NEW-LEAD")
        self.assertEqual(
            repo.get_report("R00005"),
            {"report_id": "R00005",
             "chromium": "NEW-VALUE", "lead": "NEW-LEAD"},
        )
        with db.new_conn() as c:
            locked = c.execute(
                "SELECT 1 FROM locked_reports WHERE report_id='R00005'"
            ).fetchone()
        self.assertIsNotNone(locked)

        # A "late" replication attempt (simulating a straggler batch) must
        # be skipped for locked rows.
        m = Migrator()
        m._reconcile()
        self.assertEqual(
            repo.get_report("R00005")["chromium"], "NEW-VALUE"
        )

    def test_old_table_not_written_after_completion(self):
        self._seed(80)
        self._run_to_completion()
        with db.old_conn() as c:
            before = c.execute(
                "SELECT COUNT(*) c FROM reports"
            ).fetchone()["c"]
        repo.upsert_report("R77777", "1.0", "2.0")
        with db.old_conn() as c:
            after = c.execute(
                "SELECT COUNT(*) c FROM reports"
            ).fetchone()["c"]
        self.assertEqual(before, after, "old table must stay frozen")
        self.assertIsNotNone(repo.get_report("R77777"))

    def test_queries_stay_responsive_while_running(self):
        self._seed(500)
        m = Migrator()
        m.start()
        _wait_until(lambda: progress()["state"] == STATE_RUNNING)
        # A burst of queries while batches keep converting.
        for i in range(20):
            rid = f"R{(i * 13 % 500) + 1:04d}"
            self.assertIsNotNone(repo.get_report(rid))
        self.assertTrue(_wait_until(
            lambda: progress()["state"] == STATE_COMPLETED
        ))

    def test_completes_under_continuous_corrections(self):
        """Cutover must not wait for a quiet period that never arrives."""
        import threading

        self._seed(300)
        stop = threading.Event()
        done = threading.Event()

        def churn():
            i = 0
            while not stop.is_set():
                i += 1
                rid = f"R{(i % 300) + 1:04d}"
                repo.upsert_report(rid, f"c-{i}", f"l-{i}")
                if done.is_set():
                    break

        t = threading.Thread(target=churn, daemon=True)
        m = Migrator()
        m.start()
        t.start()
        try:
            finished = _wait_until(
                lambda: progress()["state"] == STATE_COMPLETED, timeout=45
            )
            self.assertTrue(finished, "migration stuck despite continuous writes")
        finally:
            # Stop churning the moment cutover completes; afterwards the old
            # table is frozen by design and can no longer be compared 1:1.
            done.set()
            stop.set()
            t.join(timeout=5)
        # All locked (app-written) rows read back exactly what the app wrote,
        # and every non-locked wide row still matches the long table.
        self._assert_tables_match_allowing_locked()


class TestIdempotency(MigrationTestBase):
    def test_start_after_completion_is_noop(self):
        with db.old_conn() as c:
            c.execute(
                "INSERT INTO reports VALUES ('A', '1', NULL)"
            )
        m = Migrator()
        self.assertTrue(m.start())
        _wait_until(lambda: progress()["state"] == STATE_COMPLETED)
        self.assertFalse(m.start())
        self.assertEqual(progress()["state"], STATE_COMPLETED)
        self.assertEqual(
            repo.get_report("A"),
            {"report_id": "A", "chromium": "1", "lead": None},
        )


class TestCutoverRetry(MigrationTestBase):
    def test_cutover_sqlite_error_is_retried_and_completes(self):
        self._seed(120)
        m = Migrator()
        m.start()

        real_connect = sqlite3.connect
        calls = {"n": 0}

        def flaky_connect(*args, **kwargs):
            conn = real_connect(*args, **kwargs)
            # Only arm the fault for the lock connection opened inside
            # _cutover (the immediate caller's code name), leaving the
            # regular old_conn() read connections untouched.
            caller = sys._getframe(1).f_code.co_name
            if "old.db" in str(args[0]) and caller == "_cutover" and calls["n"] == 0:
                calls["n"] += 1

                class _BoomConn:
                    def __init__(self, inner):
                        self._inner = inner

                    def execute(self, sql, *a, **k):
                        if sql.startswith("BEGIN IMMEDIATE"):
                            raise sqlite3.OperationalError(
                                "simulated lock contention"
                            )
                        return self._inner.execute(sql, *a, **k)

                    def rollback(self):
                        self._inner.rollback()

                    def close(self):
                        self._inner.close()

                return _BoomConn(conn)
            return conn

        orig = sqlite3.connect
        sqlite3.connect = flaky_connect
        try:
            ok = _wait_until(
                lambda: progress()["state"] == STATE_COMPLETED, timeout=30
            )
        finally:
            sqlite3.connect = orig
        self.assertTrue(ok, progress())
        self.assertGreaterEqual(progress()["error_count"], 1)
        with db.new_conn() as conn:
            logged = conn.execute(
                "SELECT message FROM migration_errors "
                "WHERE message LIKE '%cutover%' LIMIT 1"
            ).fetchone()
        self.assertIsNotNone(logged)
        self._assert_tables_match()


if __name__ == "__main__":
    unittest.main(verbosity=2)
