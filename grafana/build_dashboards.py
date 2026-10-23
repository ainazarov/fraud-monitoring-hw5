"""Build the four original HW5 dashboards; uses only the Python standard library.

Run `python grafana/build_dashboards.py` from the project root after changes.
The generated JSON files are committed and mounted directly by Grafana.
"""

import copy
import json
from pathlib import Path

PROM = {"type": "prometheus", "uid": "prometheus"}
POSTGRES = {"type": "postgres", "uid": "postgres"}
COLORS = ["#73BF69", "#F2CC0C", "#FF9830", "#F2495C", "#5794F2"]
LINKS = [
    {"title": title, "url": f"/d/{uid}", "type": "link", "includeVars": False, "keepTime": True}
    for title, uid in [
        ("Scoring", "hw5-scoring"), ("Writer", "hw5-writer"),
        ("System", "hw5-system"), ("Business", "hw5-business"),
    ]
]


def prom(expr, legend="", instant=False, ref="A", format="time_series"):
    return {"refId": ref, "datasource": PROM, "expr": expr, "legendFormat": legend,
            "editorMode": "code", "range": not instant, "instant": instant,
            "format": format, "exemplar": False}


def sql(query, format="table", ref="A"):
    return {"refId": ref, "datasource": POSTGRES, "rawSql": query.strip(),
            "format": format, "editorMode": "code", "rawQuery": True}


def panel(id, title, kind, x, y, w, h, targets, unit="short", description="", options=None):
    defaults = {"unit": unit, "color": {"mode": "palette-classic"},
                "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}]},
                "mappings": [], "noValue": "No data"}
    if kind == "timeseries":
        defaults["custom"] = {"drawStyle": "line", "lineInterpolation": "smooth", "lineWidth": 2,
                              "fillOpacity": 12, "showPoints": "never", "spanNulls": False,
                              "axisBorderShow": False, "axisCenteredZero": False,
                              "axisColorMode": "text", "axisLabel": "", "axisPlacement": "auto",
                              "stacking": {"mode": "none", "group": "A"},
                              "thresholdsStyle": {"mode": "off"}}
        opts = {"tooltip": {"mode": "multi", "sort": "desc"},
                "legend": {"showLegend": True, "displayMode": "list", "placement": "bottom", "calcs": []}}
    elif kind == "stat":
        opts = {"reduceOptions": {"values": False, "calcs": ["lastNotNull"], "fields": ""},
                "orientation": "auto", "textMode": "auto", "colorMode": "value",
                "graphMode": "area", "justifyMode": "auto"}
        defaults["color"] = {"mode": "fixed", "fixedColor": "blue"}
    elif kind == "table":
        opts = {"showHeader": True, "cellHeight": "sm", "footer": {"show": False},
                "sortBy": []}
    elif kind == "barchart":
        opts = {"orientation": "auto", "barWidth": 0.8, "groupWidth": 0.85,
                "showValue": "auto", "stacking": "none", "xTickLabelRotation": 0,
                "legend": {"showLegend": False}, "tooltip": {"mode": "multi", "sort": "desc"}}
        defaults["custom"] = {"fillOpacity": 85, "lineWidth": 0, "axisPlacement": "auto"}
    else:
        opts = {}
    if options:
        opts.update(options)
    return {"id": id, "title": title, "type": kind, "description": description,
            "gridPos": {"x": x, "y": y, "w": w, "h": h},
            "datasource": targets[0]["datasource"] if targets else PROM,
            "pluginVersion": "11.6.0", "targets": targets,
            "fieldConfig": {"defaults": defaults, "overrides": []}, "options": opts}


def bounds(p, minimum=0, maximum=None, decimals=None):
    fields = p["fieldConfig"]["defaults"]
    fields["min"] = minimum
    if maximum is not None:
        fields["max"] = maximum
    if decimals is not None:
        fields["decimals"] = decimals
    return p


def heatmap(id, title, metric, x, y, w, h):
    return panel(id, title, "heatmap", x, y, w, h,
                 [prom(f'sum by (le) (increase({metric}_bucket{{le!="+Inf"}}[$__rate_interval]))', "{{le}}", format="heatmap")],
                 description="Score histogram in each rolling scrape window. Prometheus cumulative buckets are converted to disjoint buckets by the heatmap query format; color represents transaction count.",
                 options={"calculate": False, "cellGap": 1,
                          "color": {"mode": "scheme", "scheme": "Oranges", "steps": 64, "scale": "linear", "exponent": 0.5},
                          "legend": {"show": True}, "tooltip": {"show": True, "yHistogram": False},
                          "yAxis": {"axisPlacement": "left", "reverse": False, "unit": "none", "min": 0, "max": 1},
                          "rowsFrame": {"layout": "auto"}, "filterValues": {"le": 0}})


def annotations():
    return {"list": [{"name": "Model activations", "datasource": PROM, "enable": True,
                      "hide": False, "iconColor": "#B877D9",
                      "expr": "(fw_model_activated_timestamp_seconds > (time() - 30)) * 1000",
                      "step": "10s", "useValueForTime": True, "tagKeys": "version",
                      "titleFormat": "Model {{version}} activated",
                      "textFormat": "The scorer successfully loaded model {{version}}. The marker uses the actual activation timestamp."}]}


def dashboard(uid, title, description, panels, variables=None, model_markers=True):
    return {"id": None, "uid": uid, "title": title, "description": description,
            "tags": ["hw5", "fraud-monitoring", "original"], "timezone": "browser",
            "schemaVersion": 40, "version": 1, "editable": False, "style": "dark",
            "graphTooltip": 1, "refresh": "5s", "time": {"from": "now-15m", "to": "now"},
            "timepicker": {"refresh_intervals": ["5s", "10s", "30s", "1m", "5m"]},
            "links": copy.deepcopy(LINKS), "annotations": annotations() if model_markers else {"list": []},
            "templating": {"list": variables or []}, "panels": panels}


def scorer():
    reload_health = bounds(panel(13, "Model reload health", "stat", 0, 28, 12, 6,
                                 [prom("max(fw_model_reload_failed)", instant=True)], "short",
                                 "Last model-refresh attempt: 0 = healthy, 1 = failed. On failure the scorer retains its last valid model; a healthy TPS alone would not reveal the failed update."), 0, 1, 0)
    reload_health["fieldConfig"]["defaults"].update({
        "color": {"mode": "thresholds"},
        "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "red", "value": 1}]},
        "mappings": [{"type": "value", "options": {
            "0": {"text": "Healthy", "color": "green", "index": 0},
            "1": {"text": "Reload failed", "color": "red", "index": 1},
        }}],
    })
    return dashboard("hw5-scoring", "HW5 / Scoring pulse",
        "Scorer throughput, decisions and model changes. Process metrics count successful emitted scores; SQL panels describe durable records in the selected processing-time range.", [
        bounds(panel(1, "Scoring throughput · 30s", "stat", 0, 0, 6, 4,
                     [prom("sum(rate(fw_transactions_total[30s]))", instant=True)], "ops", "Successfully emitted scores per second, averaged over 30 seconds."), decimals=2),
        bounds(panel(2, "Processing p95 · 30s", "stat", 6, 0, 6, 4,
                     [prom("histogram_quantile(0.95, sum by (le) (rate(fw_processing_seconds_bucket[30s])))", instant=True)], "s",
                     "95th percentile of the complete scoring handler, including acknowledged output publication.")),
        bounds(panel(3, "Flagged fraction · 30s", "stat", 12, 0, 6, 4,
                     [prom("sum(rate(fw_fraud_total[30s])) / clamp_min(sum(rate(fw_transactions_total[30s])), 1e-9)", instant=True)], "percentunit",
                     "Flagged decisions / successfully scored transactions. No traffic displays zero; the fraud alert additionally requires 20 transactions per window."), 0, 1, 1),
        bounds(panel(4, "Since last score", "stat", 18, 0, 6, 4,
                     [prom('time() - max(fw_last_transaction_timestamp_seconds{job="fraud-detector"})', instant=True)], "s",
                     "Wall-clock age of the last successful scoring. The stream-stall rule fires above 60 seconds for 30 seconds.")),
        bounds(panel(5, "Decision flow", "timeseries", 0, 4, 12, 8,
                     [prom("sum(rate(fw_transactions_total[$__rate_interval]))", "all scores"),
                      prom("sum(rate(fw_fraud_total[$__rate_interval]))", "flagged", ref="B")], "ops")),
        bounds(panel(6, "Handler latency", "timeseries", 12, 4, 12, 8,
                     [prom("histogram_quantile(0.50, sum by (le) (rate(fw_processing_seconds_bucket[$__rate_interval])))", "p50"),
                      prom("histogram_quantile(0.95, sum by (le) (rate(fw_processing_seconds_bucket[$__rate_interval])))", "p95", ref="B"),
                      prom("histogram_quantile(0.99, sum by (le) (rate(fw_processing_seconds_bucket[$__rate_interval])))", "p99", ref="C")], "s")),
        heatmap(7, "Score distribution over time", "fw_score", 0, 12, 12, 8),
        bounds(panel(8, "Flagged decisions in selected range", "stat", 12, 12, 6, 4,
                     [prom("sum(increase(fw_fraud_total[$__range]))", instant=True)], "short",
                     "Estimated interval increase from Prometheus counters. SQL tables provide exact committed counts."), decimals=0),
        bounds(panel(9, "Rejected inputs in selected range", "stat", 18, 12, 6, 4,
                     [prom('sum(increase(fw_rejected_total{service="scorer"}[$__range]))', instant=True)], "short",
                     "Malformed inputs rejected by the scoring service."), decimals=0),
        panel(10, "Model activation history", "table", 12, 16, 12, 4,
              [prom("fw_model_activations_total", "{{version}}", instant=True, format="table")], "short",
              "Counter labels identify each successfully loaded version; purple annotations mark the actual load timestamp."),
        panel(11, "Durable score counts by model", "barchart", 0, 20, 12, 8,
              [sql("SELECT model_version, count(*) AS transactions FROM scores WHERE $__timeFilter(processed_at) GROUP BY model_version ORDER BY transactions DESC")], "short",
              "Exact PostgreSQL counts in the selected processing-time range."),
        panel(12, "Durable fraud rate by model", "table", 12, 20, 12, 8,
              [sql("SELECT model_version, count(*) AS transactions, sum(fraud_flag) AS flagged, avg(fraud_flag::double precision) AS fraud_rate, avg(score) AS mean_score FROM scores WHERE $__timeFilter(processed_at) GROUP BY model_version ORDER BY transactions DESC")], "short",
              "Compare model exposure and flagged fractions. Rate is flagged / all durable scores for each version."),
        reload_health,
        bounds(panel(14, "Model reload failures · rolling 1m", "timeseries", 12, 28, 12, 6,
                     [prom("sum(increase(fw_model_reload_failures_total[1m]))", "failed attempts / 1m")], "short",
                     "Failed model-refresh attempts in each rolling minute. A persistent invalid artifact can fail once per incoming transaction; these are attempts, not unique failed versions. Nonzero values expose a degraded deployment even when the last valid model keeps scoring.")),
    ])


def writer():
    return dashboard("hw5-writer", "HW5 / Durable writes",
        "Delivery and persistence health. The writer counts only newly committed rows; duplicate replay attempts are measured separately.", [
        bounds(panel(1, "Committed throughput · 30s", "stat", 0, 0, 6, 4,
                     [prom("sum(rate(fw_scores_written_total[30s]))", instant=True)], "ops"), decimals=2),
        bounds(panel(2, "Write p95 · 30s", "stat", 6, 0, 6, 4,
                     [prom("histogram_quantile(0.95, sum by (le) (rate(fw_write_seconds_bucket[30s])))", instant=True)], "s",
                     "Database operation plus commit duration.")),
        bounds(panel(3, "Persisted flagged fraction · 30s", "stat", 12, 0, 6, 4,
                     [prom("sum(rate(fw_written_fraud_total[30s])) / clamp_min(sum(rate(fw_scores_written_total[30s])), 1e-9)", instant=True)], "percentunit"), 0, 1, 1),
        bounds(panel(4, "Duplicate replays in selected range", "stat", 18, 0, 6, 4,
                     [prom("sum(increase(fw_write_duplicates_total[$__range]))", instant=True)], "short",
                     "First insert wins; these attempts do not increment committed score counters."), decimals=0),
        bounds(panel(5, "Scoring versus durable writes", "timeseries", 0, 4, 12, 8,
                     [prom("sum(rate(fw_transactions_total[$__rate_interval]))", "scored"),
                      prom("sum(rate(fw_scores_written_total[$__rate_interval]))", "committed", ref="B")], "ops",
                     "Compare rates to find a growing persistence gap. Counters reset independently when processes restart.")),
        bounds(panel(6, "Database and delivery latency", "timeseries", 12, 4, 12, 8,
                     [prom("histogram_quantile(0.95, sum by (le) (rate(fw_write_seconds_bucket[$__rate_interval])))", "DB write p95"),
                      prom("histogram_quantile(0.95, sum by (le) (rate(fw_end_to_end_seconds_bucket[$__rate_interval])))", "emission-to-commit p95", ref="B")], "s",
                     "End-to-end latency runs from emitted_at (live producer wall clock) to a successful database commit; historical transaction_time is never used for live latency.")),
        heatmap(7, "Persisted score distribution over time", "fw_written_score", 0, 12, 12, 8),
        bounds(panel(8, "Committed flagged decisions in selected range", "stat", 12, 12, 6, 4,
                     [prom("sum(increase(fw_written_fraud_total[$__range]))", instant=True)], "short"), decimals=0),
        bounds(panel(9, "Rejected results in selected range", "stat", 18, 12, 6, 4,
                     [prom('sum(increase(fw_rejected_total{service="writer"}[$__range]))', instant=True)], "short"), decimals=0),
        panel(10, "Exact stored totals in selected range", "table", 12, 16, 12, 4,
              [sql("SELECT count(*) AS committed, coalesce(sum(fraud_flag),0) AS flagged, count(DISTINCT model_version) AS models FROM scores WHERE $__timeFilter(scored_at)")],
              description="Database insertion time is used here so this panel reflects durable-write activity."),
        panel(11, "Latest committed scores", "table", 0, 20, 24, 9,
              [sql("SELECT transaction_id, model_version, score, fraud_flag, us_state, merch, cat_id, amount, processed_at, scored_at, extract(epoch FROM (scored_at-processed_at)) AS delivery_seconds FROM scores WHERE $__timeFilter(scored_at) ORDER BY sequence DESC LIMIT 100")],
              description="Newest 100 committed transactions in the selected write-time range. Transaction context is stored in PostgreSQL, never as high-cardinality metric labels."),
    ])


def system():
    instance = 'instance=~"$instance"'
    cpu = f'100 * (1 - avg by (instance) (rate(node_cpu_seconds_total{{{instance},mode="idle"}}[$__rate_interval])))'
    memory = f'100 * (1 - node_memory_MemAvailable_bytes{{{instance}}} / node_memory_MemTotal_bytes{{{instance}}})'
    fs = instance + ',fstype!~"tmpfs|overlay|squashfs|nsfs|ramfs|devtmpfs",mountpoint!~"/run.*|/var/lib/docker.*"'
    disk = f'100 * (1 - node_filesystem_avail_bytes{{{fs}}} / node_filesystem_size_bytes{{{fs}}})'
    variable = {"name": "instance", "label": "Host", "type": "query", "datasource": PROM,
                "query": {"query": 'label_values(node_uname_info{job="node-exporter"}, instance)', "refId": "instance"},
                "definition": 'label_values(node_uname_info{job="node-exporter"}, instance)',
                "refresh": 1, "sort": 1, "multi": False, "includeAll": False,
                "current": {"text": "node-exporter:9100", "value": "node-exporter:9100"}, "options": []}
    return dashboard("hw5-system", "HW5 / Compute health",
        "Original node-exporter overview of the Docker host/WSL Linux system: CPU, available memory, network, disk space and disk I/O. This is host telemetry, not per-container utilization.", [
        bounds(panel(1, "CPU busy", "stat", 0, 0, 6, 4, [prom(cpu, instant=True)], "percent"), 0, 100, 1),
        bounds(panel(2, "RAM used", "stat", 6, 0, 6, 4, [prom(memory, instant=True)], "percent",
                     "Uses MemAvailable, allowing the Linux reclaimable page cache."), 0, 100, 1),
        bounds(panel(3, "Busiest filesystem", "stat", 12, 0, 6, 4, [prom(f"max({disk})", instant=True)], "percent"), 0, 100, 1),
        bounds(panel(4, "Exporter availability", "stat", 18, 0, 6, 4,
                     [prom(f'up{{job="node-exporter",{instance}}}', instant=True)], "short"), 0, 1, 0),
        bounds(panel(5, "CPU busy over time", "timeseries", 0, 4, 12, 8, [prom(cpu, "{{instance}}")], "percent"), 0, 100),
        bounds(panel(6, "RAM used over time", "timeseries", 12, 4, 12, 8, [prom(memory, "{{instance}}")], "percent"), 0, 100),
        bounds(panel(7, "Network receive / transmit", "timeseries", 0, 12, 12, 8,
                     [prom(f'rate(node_network_receive_bytes_total{{{instance},device!~"lo|veth.*|docker.*|br-.*"}}[$__rate_interval])', "{{device}} receive"),
                      prom(f'rate(node_network_transmit_bytes_total{{{instance},device!~"lo|veth.*|docker.*|br-.*"}}[$__rate_interval])', "{{device}} transmit", ref="B")], "Bps")),
        bounds(panel(8, "Filesystem space used", "timeseries", 12, 12, 12, 8, [prom(disk, "{{mountpoint}} ({{device}})")], "percent"), 0, 100),
        bounds(panel(9, "Disk read / write traffic", "timeseries", 0, 20, 12, 8,
                     [prom(f'rate(node_disk_read_bytes_total{{{instance},device!~"loop.*|ram.*"}}[$__rate_interval])', "{{device}} read"),
                      prom(f'rate(node_disk_written_bytes_total{{{instance},device!~"loop.*|ram.*"}}[$__rate_interval])', "{{device}} write", ref="B")], "Bps")),
        bounds(panel(10, "CPU load", "timeseries", 12, 20, 12, 8,
                     [prom(f'node_load1{{{instance}}}', "1 minute"), prom(f'node_load5{{{instance}}}', "5 minutes", ref="B"),
                      prom(f'node_load15{{{instance}}}', "15 minutes", ref="C")], "short")),
        panel(11, "All scrape targets", "table", 0, 28, 24, 5, [prom("up", instant=True, format="table")],
              description="1 = reachable; 0 = failed scrape. Scraper HTTP success does not guarantee transaction progress; use the scoring stall alert."),
    ], [variable], model_markers=False)


FILTERS = """('__all' IN (${us_state:sqlstring}) OR coalesce(nullif(us_state,''),'unknown') IN (${us_state:sqlstring}))
  AND ('__all' IN (${merch:sqlstring}) OR coalesce(nullif(merch,''),'unknown') IN (${merch:sqlstring}))"""


def business_variable(name, label, column):
    # Explicit All row keeps an empty warehouse valid; sqlstring escapes values,
    # including merchant names containing apostrophes. No raw SQL interpolation.
    query = f"SELECT label AS __text, value AS __value FROM (SELECT 'All' AS label, '__all' AS value UNION SELECT coalesce(nullif({column},''),'unknown'), coalesce(nullif({column},''),'unknown') FROM scores) options ORDER BY CASE WHEN value='__all' THEN 0 ELSE 1 END, label"
    return {"name": name, "label": label, "type": "query", "datasource": POSTGRES,
            "query": query, "definition": query, "refresh": 1, "sort": 0, "multi": True,
            "includeAll": False, "current": {"text": ["All"], "value": ["__all"]}, "options": []}


def business():
    where = f"$__timeFilter(processed_at) AND {FILTERS}"
    # The upper endpoint belongs to the final bin. Every bin is width .05;
    # integral(density) = sum(bin_count / n / .05 * .05) = 1 for n > 0.
    density = f"""WITH filtered AS (
  SELECT least(floor(score * 20)::integer, 19) AS bin FROM scores WHERE {where}
), counts AS (SELECT bin, count(*) AS n FROM filtered GROUP BY bin),
total AS (SELECT count(*)::double precision AS n FROM filtered)
SELECT to_char(b.bin * 0.05, '0.00') || '-' || to_char((b.bin + 1) * 0.05, '0.00') AS score_bin,
       coalesce(c.n,0) / nullif(t.n * 0.05,0) AS density
FROM generate_series(0,19) b(bin) CROSS JOIN total t LEFT JOIN counts c USING (bin)
ORDER BY b.bin"""
    # All buckets have their actual selected-window exposure as denominator;
    # partial boundary buckets are not divided by a full interval.
    tps = f"""WITH settings AS (
  SELECT greatest($__interval_ms / 1000.0, 1) AS width,
         extract(epoch FROM $__timeFrom()::timestamptz) AS start_s,
         extract(epoch FROM $__timeTo()::timestamptz) AS end_s
), buckets AS (
  SELECT generate_series(floor(start_s/width)::bigint, ceil(end_s/width)::bigint-1) AS bucket,
         width, start_s, end_s FROM settings
), counts AS (
  SELECT floor(extract(epoch FROM processed_at) / s.width)::bigint AS bucket, count(*) AS n
  FROM scores CROSS JOIN settings s WHERE {where} GROUP BY 1
)
SELECT to_timestamp(greatest(b.bucket*b.width,b.start_s)) AS time,
       coalesce(c.n,0)::double precision / nullif(least((b.bucket+1)*b.width,b.end_s)-greatest(b.bucket*b.width,b.start_s),0) AS tps
FROM buckets b LEFT JOIN counts c USING (bucket) ORDER BY time"""
    categories = f"""WITH latest AS (
  SELECT cat_id, fraud_flag, us_state, merch FROM scores ORDER BY sequence DESC LIMIT 1000
)
SELECT coalesce(nullif(cat_id,''),'unknown') AS category,
       avg(fraud_flag::double precision) AS fraud_fraction
FROM latest WHERE {FILTERS}
GROUP BY 1 ORDER BY fraud_fraction DESC, category"""
    selected_stats = [
        (1, "Selected transactions", "count(*)", "short"),
        (2, "Selected flagged fraction", "avg(fraud_flag::double precision)", "percentunit"),
        (3, "Selected flagged amount", "coalesce(sum(amount) FILTER (WHERE fraud_flag=1),0)", "currencyUSD"),
        (4, "Selected mean score", "avg(score)", "none"),
    ]
    panels = [bounds(panel(id, title, "stat", (id-1)*6, 0, 6, 4,
                          [sql(f"SELECT {expression} AS value FROM scores WHERE {where}")], unit,
                          "Selected processing-time range, state and merchant filters."))
              for id, title, expression, unit in selected_stats]
    panels += [
        panel(5, "Filtered score probability density", "barchart", 0, 4, 12, 9, [sql(density)], "none",
              "20 equal bins of width 0.05. Density = bin count / (filtered sample size × 0.05), so area sums to one; density can exceed one. Score 1 belongs to the last bin. Empty selection has no defined density.",
              {"xTickLabelRotation": -45, "showValue": "never"}),
        bounds(panel(6, "Filtered processing TPS", "timeseries", 12, 4, 12, 9, [sql(tps, "time_series")], "ops",
                     "Counts durable transactions by processed_at, the scorer's actual wall-clock processing time, not historical transaction_time or database insertion time. Empty buckets are zero; partial boundary buckets use their actual duration.")),
        bounds(panel(7, "Fraud fraction by category · latest 1,000 overall", "barchart", 0, 13, 12, 9,
                     [sql(categories)], "percentunit",
                     "First select the most recent 1,000 committed transactions globally using sequence DESC; then apply state/merchant filters; then compute flagged / category sample size. This rolling population intentionally ignores the dashboard time picker.",
                     {"xTickLabelRotation": -25}), 0, 1),
        bounds(panel(8, "Flagged amount by state", "barchart", 12, 13, 12, 9,
                     [sql(f"SELECT coalesce(nullif(us_state,''),'unknown') AS state, sum(amount) AS flagged_amount FROM scores WHERE {where} AND fraud_flag=1 GROUP BY 1 ORDER BY flagged_amount DESC LIMIT 15")], "currencyUSD",
                     "Exposure indicator: sum of amounts for flagged transactions by state, in the selected processing-time range and current filters. These are model flags, not confirmed losses.")),
        panel(9, "Merchant exposure", "table", 0, 22, 12, 8,
              [sql(f"SELECT coalesce(nullif(merch,''),'unknown') AS merchant, count(*) AS transactions, sum(fraud_flag) AS flagged, avg(fraud_flag::double precision) AS fraud_fraction, coalesce(sum(amount) FILTER (WHERE fraud_flag=1),0) AS flagged_amount FROM scores WHERE {where} GROUP BY 1 ORDER BY flagged_amount DESC, transactions DESC LIMIT 20")],
              description="Top 20 merchants by flagged amount. Small samples may produce unstable fraud fractions; inspect the transaction count."),
        panel(10, "Category sample sizes · latest 1,000 overall", "table", 12, 22, 12, 8,
              [sql(f"WITH latest AS (SELECT cat_id, fraud_flag, us_state, merch FROM scores ORDER BY sequence DESC LIMIT 1000) SELECT coalesce(nullif(cat_id,''),'unknown') AS category, count(*) AS transactions, sum(fraud_flag) AS flagged FROM latest WHERE {FILTERS} GROUP BY 1 ORDER BY transactions DESC")],
              description="Denominators for the rolling category chart. Uses the same global 1,000-row window before merchant/state filtering and ignores the dashboard time picker."),
        panel(11, "Explore recent selected transactions", "table", 0, 30, 24, 9,
              [sql(f"SELECT transaction_id, processed_at, transaction_time, model_version, score, fraud_flag, us_state, merch, cat_id, amount FROM scores WHERE {where} ORDER BY processed_at DESC, sequence DESC LIMIT 100")],
              description="Latest 100 matches. processed_at is operational time; transaction_time is the source event timestamp."),
    ]
    return dashboard("hw5-business", "HW5 / Fraud investigation",
        "State and merchant exploration with PostgreSQL filters. Standard panels follow processing time; the explicitly labelled category panels always use the global most recent 1,000 committed rows before filtering. No merchant, state or transaction identifiers are Prometheus labels.",
        panels, [business_variable("us_state", "State", "us_state"), business_variable("merch", "Merchant", "merch")])


def set_table_units(d):
    for p in d["panels"]:
        if p["type"] != "table":
            continue
        units = {"fraud_rate": "percentunit", "fraud_fraction": "percentunit", "amount": "currencyUSD",
                 "flagged_amount": "currencyUSD", "score": "none", "mean_score": "none", "delivery_seconds": "s"}
        p["fieldConfig"]["overrides"] = [
            {"matcher": {"id": "byName", "options": name}, "properties": [{"id": "unit", "value": unit}]}
            for name, unit in units.items()
        ]


def main():
    destination = Path(__file__).parent / "dashboards"
    destination.mkdir(exist_ok=True)
    for name, d in [("scoring", scorer()), ("writer", writer()), ("system", system()), ("business", business())]:
        set_table_units(d)
        (destination / f"{name}.json").write_text(json.dumps(d, indent=2) + "\n", encoding="utf-8")
        print(f"{name}: {d['uid']}, {len(d['panels'])} panels")


if __name__ == "__main__":
    main()
