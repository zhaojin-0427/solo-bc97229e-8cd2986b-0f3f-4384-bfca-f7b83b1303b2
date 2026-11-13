"""Seed the legacy wide table with demo data.

Idempotent: only inserts report_ids that do not already exist, so restarting
the container never overwrites corrections made through the web UI.
"""
from __future__ import annotations

import random

from .db import old_conn

DEMO_ROWS = 1200


def seed() -> int:
    random.seed(20261113)
    inserted = 0
    with old_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            for i in range(1, DEMO_ROWS + 1):
                rid = f"R{i:05d}"
                exists = conn.execute(
                    "SELECT 1 FROM reports WHERE report_id = ?", (rid,)
                ).fetchone()
                if exists:
                    continue
                # Mix of normal values, empty strings and NULLs.
                chromium = (
                    None
                    if i % 17 == 0
                    else ("" if i % 23 == 0 else f"{random.uniform(0.01, 2.5):.3f}")
                )
                lead = (
                    None
                    if i % 19 == 0
                    else ("" if i % 29 == 0 else f"{random.uniform(0.005, 1.2):.3f}")
                )
                conn.execute(
                    "INSERT INTO reports(report_id, chromium, lead) "
                    "VALUES (?, ?, ?)",
                    (rid, chromium, lead),
                )
                inserted += 1
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    return inserted


if __name__ == "__main__":
    from .db import init_dbs

    init_dbs()
    n = seed()
    print(f"seeded {n} new report rows")
