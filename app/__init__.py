"""Flask application factory for the wide->long migration console.

The Flask import is optional so the migration engine submodules (``app.db``,
``app.migrator``, ``app.repository``) remain importable in stdlib-only
environments such as the isolated test runner.
"""
from __future__ import annotations

from .db import init_dbs
from .migrator import migrator
from .seed import seed

try:
    from .web import web
except ImportError:  # pragma: no cover - exercised by stdlib-only tests
    web = None


def create_app(seed_demo: bool = True):
    init_dbs()
    if seed_demo:
        # Idempotent: preserves rows already corrected through the UI.
        seed()
    if web is None:  # pragma: no cover
        raise RuntimeError("Flask is not installed")
    # Continue an interrupted migration after container / process restart.
    migrator.resume_if_needed()
    return web
