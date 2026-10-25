#!/usr/bin/env python3
"""Prints the build plan's section-3 target metrics from Prometheus.
Usage: python3 scripts/bench_report.py [window, default 10m] [prometheus URL]"""
import json
import sys
import urllib.parse
import urllib.request

W = sys.argv[1] if len(sys.argv) > 1 else "10m"
PROM = sys.argv[2] if len(sys.argv) > 2 else "http://localhost:9091"

QUERIES = [
    ("events accepted /s", f'sum(rate(hookline_events_ingested_total{{result="accepted"}}[{W}]))'),
    ("delivery attempts /s", f"sum(rate(hookline_delivery_attempts_total[{W}]))"),
    ("success rate", f'sum(rate(hookline_delivery_attempts_total{{outcome="success"}}[{W}])) / sum(rate(hookline_delivery_attempts_total[{W}]))'),
    ("ingest p50 server-side (s)", f"histogram_quantile(0.50, sum by (le) (rate(hookline_ingest_request_duration_seconds_bucket[{W}])))"),
    ("ingest p99 server-side (s)", f"histogram_quantile(0.99, sum by (le) (rate(hookline_ingest_request_duration_seconds_bucket[{W}])))"),
    ("end-to-end p50 (s)", f"histogram_quantile(0.50, sum by (le) (rate(hookline_delivery_end_to_end_seconds_bucket[{W}])))"),
    ("end-to-end p99 (s)", f"histogram_quantile(0.99, sum by (le) (rate(hookline_delivery_end_to_end_seconds_bucket[{W}])))"),
    ("schedule lag p99 (s)", f"histogram_quantile(0.99, sum by (le) (rate(hookline_delivery_schedule_lag_seconds_bucket[{W}])))"),
    ("HTTP attempt p99 (s)", f"histogram_quantile(0.99, sum by (le) (rate(hookline_delivery_attempt_duration_seconds_bucket[{W}])))"),
    ("max outbox pending", f"max(max_over_time(hookline_outbox_pending[{W}]))"),
    ("max worker in-flight", f"max(max_over_time(hookline_worker_inflight[{W}]))"),
    ("DB pool wait s/s", f'sum by (service) (rate(hookline_db_pool_acquire_seconds_total[{W}]))'),
    ("max RSS MiB", f'max by (service) (max_over_time(process_resident_memory_bytes{{service!=""}}[{W}])) / 1048576'),
    ("CPU cores (avg)", f'sum by (service) (rate(process_cpu_seconds_total{{service!=""}}[{W}]))'),
]

for name, q in QUERIES:
    url = PROM + "/api/v1/query?" + urllib.parse.urlencode({"query": q})
    try:
        res = json.load(urllib.request.urlopen(url, timeout=10))["data"]["result"]
        vals = ", ".join(f"{r['metric'].get('service', '')} {float(r['value'][1]):.4g}".strip() for r in res) or "n/a"
    except Exception as e:  # noqa: BLE001 - report and continue
        vals = f"error: {e}"
    print(f"{name:30} {vals}")
