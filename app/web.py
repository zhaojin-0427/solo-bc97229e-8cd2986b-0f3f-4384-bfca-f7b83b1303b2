"""HTTP routes: report CRUD/query, migration control and progress."""
from __future__ import annotations

from flask import Flask, jsonify, redirect, render_template, request, url_for

from .migrator import migrator, progress
from .repository import get_report, list_reports, upsert_report

web = Flask(__name__)


@web.get("/")
def index():
    reports = list_reports(limit=50)
    return render_template(
        "index.html", reports=reports, progress=progress()
    )


@web.post("/reports")
def create_or_correct():
    report_id = request.form.get("report_id", "")
    chromium = request.form.get("chromium")
    lead = request.form.get("lead")
    try:
        action = upsert_report(report_id, chromium, lead)
    except ValueError as exc:
        web.logger.warning("rejected write: %s", exc)
        return render_template(
            "error.html", message=str(exc), back=url_for("index")
        ), 400
    return redirect(url_for("index", notice=action))


@web.get("/reports/<report_id>")
def query_report(report_id: str):
    report = get_report(report_id)
    return render_template(
        "report.html", report=report, report_id=report_id
    )


@web.get("/api/reports/<report_id>")
def api_report(report_id: str):
    report = get_report(report_id)
    if report is None:
        return jsonify({"error": "not found", "report_id": report_id}), 404
    return jsonify(report)


@web.post("/migration/start")
def start_migration():
    started = migrator.start()
    return redirect(
        url_for("index", notice="started" if started else "already-done")
    )


@web.get("/api/progress")
def api_progress():
    return jsonify(progress())


@web.get("/healthz")
def healthz():
    return {"status": "ok"}
