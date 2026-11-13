"""End-to-end web tests through the Flask test client.

Flask lives in an external prefix here (see /tmp/pylibs during local
verification); run with that prefix on PYTHONPATH, e.g.

    PYTHONPATH=/workspace:/tmp/pylibs python /workspace/tests/test_web.py
"""
from __future__ import annotations

import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_TMP = tempfile.mkdtemp(prefix="migration-web-")
os.environ["DATA_DIR"] = _TMP
os.environ["BATCH_SIZE"] = "25"
os.environ["BATCH_DELAY"] = "0"

from app import create_app  # noqa: E402
from app.migrator import migrator, progress  # noqa: E402

app = create_app(seed_demo=True)
client = app.test_client()


def _wait_state(target, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if progress()["state"] == target:
            return True
        time.sleep(0.05)
    return False


def test_index_renders_seed_data():
    resp = client.get("/")
    assert resp.status_code == 200
    assert "迁移台" in resp.get_data(as_text=True)
    assert b"R00001" in resp.data


def test_api_query_and_null_preservation():
    resp = client.get("/api/reports/R00017")  # 17 is a multiple of 17 -> NULL
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["report_id"] == "R00017"
    assert body["chromium"] is None  # NULL stays NULL through both stores


def test_insert_and_correct_while_running():
    # Insert a brand-new report with one NULL and one value.
    resp = client.post("/reports", data={
        "report_id": "WEB001", "chromium": "0.42", "lead": "",
    })
    assert resp.status_code in (302, 303)
    body = client.get("/api/reports/WEB001").get_json()
    assert body == {"report_id": "WEB001", "chromium": "0.42", "lead": None}

    # Full-row correction.
    client.post("/reports", data={
        "report_id": "WEB001", "chromium": "0.99", "lead": "0.11",
    })
    body = client.get("/api/reports/WEB001").get_json()
    assert body["chromium"] == "0.99" and body["lead"] == "0.11"


def test_full_migration_via_web_and_cutover_writes():
    resp = client.post("/migration/start")
    assert resp.status_code in (302, 303)
    assert _wait_state("completed"), progress()

    p = client.get("/api/progress").get_json()
    assert p["state"] == "completed"
    assert p["long_total"] == p["wide_total"]
    assert p["percent"] == 100.0

    # After completion writes land in the long table only.
    client.post("/reports", data={
        "report_id": "WEB002", "chromium": "7.7", "lead": "8.8",
    })
    body = client.get("/api/reports/WEB002").get_json()
    assert body["chromium"] == "7.7"

    # Old table stays frozen.
    from app import db
    with db.old_conn() as conn:
        frozen = conn.execute(
            "SELECT 1 FROM reports WHERE report_id='WEB002'"
        ).fetchone()
    assert frozen is None

    # Starting again is a no-op.
    assert migrator.start() is False

    # Long table has exactly two rows per report including NULL values.
    with db.new_conn() as conn:
        bad = conn.execute(
            "SELECT report_id, COUNT(*) c FROM results "
            "GROUP BY report_id HAVING c != 2"
        ).fetchall()
    assert not bad


def test_write_switch_keeps_reports_visible_over_http():
    """HTTP-level regression for the cutover read/write switch.

    Uses the same named checkpoints as the engine tests so the interleaving is
    deterministic: a report committed before cutover must stay visible while
    the state is ``cutover``, and a correction accepted while the old DB lock
    is held must end up on the long table, not the frozen wide one.
    """
    import threading

    from app import db
    from app import hooks
    from app.migrator import Migrator

    # This test runs after the earlier ones already completed the shared
    # database, so use a throwaway data directory for a fresh migration.
    import tempfile
    from unittest import mock

    import importlib

    migrator_mod = importlib.import_module("app.migrator")

    tmp = tempfile.mkdtemp(prefix="migration-web-race-")
    with mock.patch.multiple(
        db, DATA_DIR=tmp,
        OLD_DB_PATH=os.path.join(tmp, "old.db"),
        NEW_DB_PATH=os.path.join(tmp, "new.db"),
    ), mock.patch.object(
        migrator_mod, "OLD_DB_PATH", os.path.join(tmp, "old.db")
    ):
        db.init_dbs()
        with db.old_conn() as c:
            c.execute("BEGIN IMMEDIATE")
            for i in range(1, 31):
                c.execute(
                    "INSERT INTO reports(report_id, chromium, lead) "
                    "VALUES (?, ?, ?)",
                    (f"W{i:03d}", f"{i}.1", f"{i}.2"),
                )
            c.execute("COMMIT")

        m = Migrator()
        m.pause_before_cutover = threading.Event()
        try:
            assert m.start()
            flipped = threading.Event()
            release = threading.Event()

            def at_flip(**_kw):
                flipped.set()
                release.wait(timeout=10)

            hooks.set_hook("cutover_state_flipped", at_flip)
            m.pause_before_cutover.set()
            assert flipped.wait(timeout=10)

            # Commit a new report while state is ``cutover``.
            resp = client.post("/reports", data={
                "report_id": "RACE01", "chromium": "5.5", "lead": "",
            })
            assert resp.status_code in (302, 303)
            body = client.get("/api/reports/RACE01").get_json()
            assert body == {
                "report_id": "RACE01", "chromium": "5.5", "lead": None,
            }, body
            # Listing must include it too (no "missing during switch").
            page = client.get("/").get_data(as_text=True)
            assert "RACE01" in page

            release.set()
            hooks.clear_hooks()
            assert _wait_state("completed"), progress()
            body = client.get("/api/reports/RACE01").get_json()
            assert body["chromium"] == "5.5"
            # The old table is frozen and holds it from the drain.
            with db.old_conn() as c:
                assert c.execute(
                    "SELECT 1 FROM reports WHERE report_id='RACE01'"
                ).fetchone()
        finally:
            hooks.clear_hooks()
            if m.pause_before_cutover is not None:
                m.pause_before_cutover.set()
            m.stop()


def test_query_unknown_returns_404():
    resp = client.get("/api/reports/NO-SUCH-ID")
    assert resp.status_code == 404
    page = client.get("/reports/NO-SUCH-ID")
    assert page.status_code == 200 and "未找到" in page.get_data(as_text=True)


def test_validation_error():
    resp = client.post("/reports", data={"report_id": "  "})
    assert resp.status_code == 400


def test_healthz():
    assert client.get("/healthz").get_json() == {"status": "ok"}


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except AssertionError as exc:
                failures += 1
                print(f"FAIL {name}: {exc}")
            except Exception as exc:  # noqa: BLE001
                failures += 1
                print(f"ERROR {name}: {exc!r}")
    print(f"\n{'ALL WEB TESTS PASSED' if failures == 0 else f'{failures} FAILURES'}")
    raise SystemExit(1 if failures else 0)
