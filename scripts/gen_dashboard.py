#!/usr/bin/env python3
"""Generates monitoring/grafana/dashboards/eventflow.json.

Run `python3 scripts/gen_dashboard.py` after editing panels here; the JSON is
committed so Grafana provisioning works without running this script.
"""
import json
import pathlib

DS = {"type": "prometheus", "uid": "prometheus"}
panels = []
_id = 0


def nid():
    global _id
    _id += 1
    return _id


def row(title, y):
    panels.append({"type": "row", "title": title, "id": nid(), "collapsed": False,
                   "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}, "panels": []})


def stat(title, expr, x, y, w=4, unit="short", desc="", thresholds=None, decimals=None):
    p = {
        "type": "stat", "title": title, "id": nid(), "datasource": DS, "description": desc,
        "gridPos": {"h": 4, "w": w, "x": x, "y": y},
        "targets": [{"refId": "A", "expr": expr, "datasource": DS, "instant": False}],
        "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "colorMode": "value", "graphMode": "area", "textMode": "auto"},
        "fieldConfig": {"defaults": {"unit": unit, "thresholds": thresholds or {
            "mode": "absolute", "steps": [{"color": "green", "value": None}]}}, "overrides": []},
    }
    if decimals is not None:
        p["fieldConfig"]["defaults"]["decimals"] = decimals
    panels.append(p)


def ts(title, targets, x, y, w=12, h=8, unit="short", desc="", stack=False):
    panels.append({
        "type": "timeseries", "title": title, "id": nid(), "datasource": DS, "description": desc,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "targets": [{"refId": chr(65 + i), "expr": e, "legendFormat": l, "datasource": DS}
                    for i, (e, l) in enumerate(targets)],
        "fieldConfig": {"defaults": {"unit": unit, "custom": {
            "drawStyle": "line", "lineWidth": 2, "fillOpacity": 15 if stack else 8, "showPoints": "never",
            "stacking": {"mode": "normal" if stack else "none", "group": "A"}}}, "overrides": []},
        "options": {"legend": {"displayMode": "table", "placement": "bottom", "calcs": ["lastNotNull", "max"]},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
    })


red_at = lambda v: {"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "red", "value": v}]}

y = 0
row("Overview", y); y += 1
stat("Submitted / s", 'sum(rate(eventflow_jobs_submitted_total[1m]))', 0, y, unit="reqps", decimals=2,
     desc="Jobs accepted by the API per second (idempotent replays excluded).")
stat("Succeeded / s", 'sum(rate(eventflow_jobs_succeeded_total[1m]))', 4, y, unit="reqps", decimals=2)
stat("Queue depth", 'sum(eventflow_queue_depth)', 8, y, desc="Undelivered + unacknowledged stream messages.")
stat("Outbox backlog", 'max(eventflow_outbox_backlog)', 12, y, thresholds=red_at(1000),
     desc="Due outbox events not yet published to Redis.")
stat("Active workers", 'max(eventflow_active_workers)', 16, y,
     thresholds={"mode": "absolute", "steps": [{"color": "red", "value": None}, {"color": "green", "value": 1}]})
stat("Dead-letter jobs", 'max(eventflow_dead_letter_jobs)', 20, y, thresholds=red_at(1))
y += 4

row("Throughput", y); y += 1
ts("Job outcomes / s", [
    ('sum(rate(eventflow_jobs_submitted_total[1m]))', "submitted"),
    ('sum(rate(eventflow_jobs_succeeded_total[1m]))', "succeeded"),
    ('sum(rate(eventflow_jobs_retried_total[1m]))', "retried"),
    ('sum(rate(eventflow_jobs_dead_lettered_total[1m]))', "dead-lettered"),
    ('sum(rate(eventflow_jobs_cancelled_total[1m]))', "cancelled while running"),
], 0, y, unit="reqps")
ts("Succeeded / s by job type", [
    ('sum by (type) (rate(eventflow_jobs_succeeded_total[1m]))', "{{type}}")], 12, y, unit="reqps", stack=True)
y += 8

row("Latency", y); y += 1
ts("Execution duration (p50 / p95 / p99)", [
    ('histogram_quantile(0.50, sum by (le) (rate(eventflow_job_execution_duration_seconds_bucket[5m])))', "p50"),
    ('histogram_quantile(0.95, sum by (le) (rate(eventflow_job_execution_duration_seconds_bucket[5m])))', "p95"),
    ('histogram_quantile(0.99, sum by (le) (rate(eventflow_job_execution_duration_seconds_bucket[5m])))', "p99"),
], 0, y, w=8, unit="s")
ts("Execution p95 by job type", [
    ('histogram_quantile(0.95, sum by (le, type) (rate(eventflow_job_execution_duration_seconds_bucket[5m])))', "{{type}}"),
], 8, y, w=8, unit="s")
ts("Queue wait (p50 / p95) by queue", [
    ('histogram_quantile(0.50, sum by (le, queue) (rate(eventflow_job_queue_wait_seconds_bucket[5m])))', "p50 {{queue}}"),
    ('histogram_quantile(0.95, sum by (le, queue) (rate(eventflow_job_queue_wait_seconds_bucket[5m])))', "p95 {{queue}}"),
], 16, y, w=8, unit="s", desc="Time from scheduled_at to the attempt starting.")
y += 8

row("Queues and outbox", y); y += 1
ts("Queue depth by queue", [('sum by (queue) (eventflow_queue_depth)', "{{queue}}"),
                            ('sum by (queue) (eventflow_queue_pending_messages)', "unacked {{queue}}")], 0, y, w=8)
ts("Jobs by state (PostgreSQL)", [('sum by (status) (eventflow_jobs_in_state)', "{{status}}")], 8, y, w=8, stack=True)
ts("Outbox", [('max(eventflow_outbox_backlog)', "backlog"),
              ('max(eventflow_outbox_oldest_due_age_seconds)', "oldest due age (s)"),
              ('sum(rate(eventflow_outbox_publish_errors_total[1m]))', "publish errors / s")], 16, y, w=8)
y += 8

row("Workers and failures", y); y += 1
ts("Worker utilisation", [('sum(eventflow_worker_inflight_jobs)', "in-flight"),
                          ('sum(eventflow_worker_concurrency)', "capacity")], 0, y, w=8)
ts("Attempt failures / s by class", [
    ('sum by (class) (rate(eventflow_job_attempt_failures_total[1m]))', "{{class}}")], 8, y, w=8, unit="reqps", stack=True)
ts("Worker infrastructure failures & recovery", [
    ('sum by (reason) (rate(eventflow_worker_failures_total[1m]))', "failure: {{reason}}"),
    ('sum by (reason) (rate(eventflow_recovered_jobs_total[1m]))', "recovered: {{reason}}"),
    ('sum(rate(eventflow_duplicate_deliveries_total[1m]))', "duplicate deliveries"),
], 16, y, w=8, unit="reqps")
y += 8

row("HTTP API", y); y += 1
ts("Requests / s by route and status", [
    ('sum by (method, route, code) (rate(eventflow_http_requests_total{route!~"/metrics|/health|/ready"}[1m]))',
     "{{method}} {{route}} {{code}}")], 0, y, unit="reqps")
ts("Request latency (p50 / p95 / p99)", [
    ('histogram_quantile(0.50, sum by (le) (rate(eventflow_http_request_duration_seconds_bucket{route=~"/api/.*"}[5m])))', "p50"),
    ('histogram_quantile(0.95, sum by (le) (rate(eventflow_http_request_duration_seconds_bucket{route=~"/api/.*"}[5m])))', "p95"),
    ('histogram_quantile(0.99, sum by (le) (rate(eventflow_http_request_duration_seconds_bucket{route=~"/api/.*"}[5m])))', "p99"),
], 12, y, unit="s")
y += 8

dashboard = {
    "uid": "eventflow-overview", "title": "EventFlow", "tags": ["eventflow"], "timezone": "browser",
    "schemaVersion": 39, "version": 1, "editable": False, "refresh": "5s",
    "time": {"from": "now-30m", "to": "now"}, "panels": panels,
    "templating": {"list": []}, "annotations": {"list": []},
}
out = pathlib.Path(__file__).resolve().parent.parent / "monitoring/grafana/dashboards/eventflow.json"
out.write_text(json.dumps(dashboard, indent=2) + "\n")
print(f"wrote {out} ({len(panels)} panels)")
