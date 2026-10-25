#!/usr/bin/env python3
"""Generates the Hookline Grafana dashboards. Run: python3 dashboards/generate.py"""
import json
import pathlib

OUT = pathlib.Path(__file__).parent
DS = {"type": "prometheus", "uid": "${datasource}"}


def panel(title, exprs, x, y, w=8, h=8, kind="timeseries", unit="short", legend=None):
    targets = []
    for i, e in enumerate(exprs if isinstance(exprs, list) else [exprs]):
        expr, fmt = e if isinstance(e, tuple) else (e, "")
        targets.append({"datasource": DS, "expr": expr, "legendFormat": fmt or legend or "__auto", "refId": chr(65 + i)})
    p = {
        "title": title, "type": kind, "datasource": DS, "targets": targets,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"}},
    }
    if kind == "stat":
        p["options"] = {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "value"}
    return p


def dashboard(uid, title, panels, tenant_var=False):
    variables = [{"name": "datasource", "type": "datasource", "query": "prometheus", "current": {"text": "Prometheus", "value": "prometheus"}}]
    if tenant_var:
        variables.append({
            "name": "tenant", "type": "query", "datasource": DS, "refresh": 2,
            "query": {"query": "label_values(hookline_events_ingested_total, tenant_id)", "refId": "tenant"},
            "definition": "label_values(hookline_events_ingested_total, tenant_id)",
        })
    return {
        "uid": uid, "title": title, "tags": ["hookline"], "timezone": "utc", "schemaVersion": 39,
        "refresh": "10s", "time": {"from": "now-30m", "to": "now"},
        "templating": {"list": variables}, "panels": panels,
    }


def q(p, metric, by="", sel=""):
    return f'histogram_quantile({p}, sum by (le{"," + by if by else ""}) (rate({metric}_bucket{sel}[5m])))'


SUCCESS = 'sum(rate(hookline_delivery_attempts_total{outcome="success"%s}[5m])) / clamp_min(sum(rate(hookline_delivery_attempts_total%s[5m])), 1e-9)'

overview = dashboard("hookline-overview", "Hookline overview", [
    panel("Events in /s", [('sum(rate(hookline_events_ingested_total{result="accepted"}[1m]))', "accepted"), ('sum(rate(hookline_events_ingested_total{result!="accepted"}[1m])) by (result)', "{{result}}")], 0, 0, unit="reqps"),
    panel("Delivery attempts /s", 'sum by (outcome) (rate(hookline_delivery_attempts_total[1m]))', 8, 0, unit="reqps", legend="{{outcome}}"),
    panel("Success rate (5m)", SUCCESS % ("", ""), 16, 0, kind="stat", unit="percentunit", h=4, w=4),
    panel("Open breakers", 'sum(hookline_circuit_breakers_open)', 20, 0, kind="stat", h=4, w=4),
    panel("Retrying backlog", 'sum(hookline_deliveries_retrying)', 16, 4, kind="stat", h=4, w=4),
    panel("Dead letters / h", 'sum(increase(hookline_deliveries_dead_total[1h]))', 20, 4, kind="stat", h=4, w=4),
    panel("End-to-end latency (ingest to success)", [(q(0.5, "hookline_delivery_end_to_end_seconds"), "p50"), (q(0.95, "hookline_delivery_end_to_end_seconds"), "p95"), (q(0.99, "hookline_delivery_end_to_end_seconds"), "p99")], 0, 8, unit="s"),
    panel("Ingest API latency", [(q(0.5, "hookline_ingest_request_duration_seconds"), "p50"), (q(0.99, "hookline_ingest_request_duration_seconds"), "p99")], 8, 8, unit="s"),
    panel("Worker pods / in-flight", [('count(up{service="worker"} == 1)', "worker pods"), ('sum(hookline_worker_inflight)', "in-flight jobs")], 16, 8),
    panel("Queue lag per tenant (pending deliveries)", 'sum by (consumer_name) ({__name__=~"(nats|jetstream)_consumer_num_pending",stream_name="DELIVERIES"})', 0, 16, w=12, legend="{{consumer_name}}"),
    panel("Outbox pending / dispatcher lag", [('max(hookline_outbox_pending)', "outbox"), ('sum({__name__=~"(nats|jetstream)_consumer_num_pending",stream_name="EVENTS"})', "events not dispatched")], 12, 16, w=12),
    panel("Breaker transitions", 'sum by (to_state) (increase(hookline_circuit_breaker_transitions_total[5m]))', 0, 24, w=12, legend="{{to_state}}"),
    panel("Attempt errors by kind", 'sum by (error_kind) (rate(hookline_delivery_attempts_total{outcome!="success"}[5m]))', 12, 24, w=12, unit="reqps", legend="{{error_kind}}"),
])

T = '{tenant_id="$tenant"}'
TS = ',tenant_id="$tenant"'
tenant = dashboard("hookline-tenant", "Hookline tenant drill-down", [
    panel("Events in /s", 'sum by (result) (rate(hookline_events_ingested_total' + T + '[1m]))', 0, 0, unit="reqps", legend="{{result}}"),
    panel("Delivery attempts /s", 'sum by (outcome) (rate(hookline_delivery_attempts_total' + T + '[1m]))', 8, 0, unit="reqps", legend="{{outcome}}"),
    panel("Success rate (5m)", SUCCESS % (TS, T), 16, 0, kind="stat", unit="percentunit"),
    panel("End-to-end latency", [(q(0.5, "hookline_delivery_end_to_end_seconds", sel=T), "p50"), (q(0.99, "hookline_delivery_end_to_end_seconds", sel=T), "p99")], 0, 8, unit="s"),
    panel("Retrying / dead", [('sum(hookline_deliveries_retrying' + T + ')', "retrying"), ('sum(increase(hookline_deliveries_dead_total' + T + '[1h]))', "dead / h")], 8, 8),
    panel("Queue lag", 'sum({__name__=~"(nats|jetstream)_consumer_num_pending",consumer_name="deliver_$tenant"})', 16, 8),
    panel("Rate limited (all tenants)", 'sum by (kind) (rate(hookline_rate_limited_total[1m]))', 0, 16, w=12, unit="reqps", legend="{{kind}}"),
    panel("Ingest rejections", 'sum(rate(hookline_events_ingested_total{result="rejected"' + TS + '}[1m]))', 12, 16, w=12, unit="reqps"),
], tenant_var=True)

infra = dashboard("hookline-infra", "Hookline infrastructure", [
    panel("NATS stream messages", 'sum by (stream_name) ({__name__=~"(nats|jetstream)_stream_total_messages"})', 0, 0, legend="{{stream_name}}"),
    panel("NATS consumer pending / ack pending", [('sum by (stream_name) ({__name__=~"(nats|jetstream)_consumer_num_pending"})', "pending {{stream_name}}"), ('sum by (stream_name) ({__name__=~"(nats|jetstream)_consumer_num_ack_pending"})', "ack pending {{stream_name}}")], 8, 0),
    panel("NATS redeliveries /s", 'sum by (stream_name) (rate({__name__=~"(nats|jetstream)_consumer_num_redelivered"}[5m]))', 16, 0),
    panel("DB pool connections", [('sum by (service) (hookline_db_pool_acquired_connections)', "in use {{service}}"), ('sum by (service) (hookline_db_pool_total_connections)', "open {{service}}")], 0, 8),
    panel("DB pool wait", 'sum by (service) (rate(hookline_db_pool_acquire_seconds_total[5m]))', 8, 8, unit="s"),
    panel("Postgres connections", [('sum(pg_stat_activity_count)', "active"), ('max(pg_settings_max_connections)', "max")], 16, 8),
    panel("gRPC latency p99", q(0.99, "hookline_grpc_server_handling_seconds", by="method"), 0, 16, unit="s", legend="{{method}}"),
    panel("Redis ops /s", 'sum(rate(redis_commands_processed_total[1m]))', 8, 16, unit="ops"),
    panel("Process memory", 'sum by (service) (process_resident_memory_bytes{service!=""})', 16, 16, unit="bytes", legend="{{service}}"),
    panel("Process CPU", 'sum by (service) (rate(process_cpu_seconds_total{service!=""}[1m]))', 0, 24, w=12, legend="{{service}}"),
    panel("Goroutines", 'sum by (service) (go_goroutines{service!=""})', 12, 24, w=12, legend="{{service}}"),
])

SLI_BAD = '1 - (sum(rate(hookline_delivery_schedule_lag_seconds_bucket{le="60"}[%s])) / clamp_min(sum(rate(hookline_delivery_schedule_lag_seconds_count[%s])), 1e-9))'
slo = dashboard("hookline-slo", "Hookline SLO", [
    panel("SLO: attempts started within 60 s of due time (30d target 99.5%)", '1 - (' + SLI_BAD % ("1h", "1h") + ')', 0, 0, w=12, kind="stat", unit="percentunit"),
    panel("Error budget remaining (window of the selected range)", '1 - ((' + SLI_BAD % ("$__range", "$__range") + ') / 0.005)', 12, 0, w=12, kind="stat", unit="percentunit"),
    panel("Burn rate", [((SLI_BAD % ("5m", "5m")) + " / 0.005", "5m"), ((SLI_BAD % ("1h", "1h")) + " / 0.005", "1h"), ((SLI_BAD % ("6h", "6h")) + " / 0.005", "6h")], 0, 8, w=24),
    panel("Schedule lag", [(q(0.5, "hookline_delivery_schedule_lag_seconds"), "p50"), (q(0.99, "hookline_delivery_schedule_lag_seconds"), "p99")], 0, 16, w=12, unit="s"),
    panel("Delivery success rate", SUCCESS % ("", ""), 12, 16, w=12, unit="percentunit"),
])

for name, d in [("overview", overview), ("tenant", tenant), ("infrastructure", infra), ("slo", slo)]:
    (OUT / f"hookline-{name}.json").write_text(json.dumps(d, indent=2) + "\n")
print("wrote 4 dashboards")
