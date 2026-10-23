"""Exercise live Kafka, PostgreSQL, Prometheus and provisioned Grafana queries.

Run in the Compose tools container. Evidence is saved in the shared storage.
No metrics are fabricated and no model publication gates are bypassed.
"""
from __future__ import annotations

import argparse
import copy
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import re
import sys
import time

import psycopg2
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline.registry import ModelRegistry, atomic_json, storage_root
from scripts.retrain import run_retraining

PROMETHEUS = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
GRAFANA = os.getenv("GRAFANA_URL", "http://grafana:3000")
SESSION = requests.Session()
SESSION.auth = ("admin", os.getenv("GRAFANA_ADMIN_PASSWORD", "admin"))


def wait_for(probe, description, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = probe()
        if result:
            return result
        time.sleep(2)
    raise AssertionError(f"Timed out: {description}")


def prom(query):
    response = requests.get(PROMETHEUS + "/api/v1/query", params={"query": query}, timeout=15)
    response.raise_for_status()
    result = response.json()
    assert result["status"] == "success", result
    return result["data"]["result"]


def scalar(query):
    results = prom(query)
    assert len(results) == 1, (query, results)
    value = float(results[0]["value"][1])
    assert math.isfinite(value), (query, value)
    return value


def grafana_get(path):
    response = SESSION.get(GRAFANA + path, timeout=30)
    response.raise_for_status()
    return response.json()


def substitute(text, state="__all", merchant="__all"):
    replacements = {
        "${us_state:sqlstring}": "'" + state.replace("'", "''") + "'",
        "${merch:sqlstring}": "'" + merchant.replace("'", "''") + "'",
        "$instance": "node-exporter:9100",
        "$__rate_interval": "30s", "$__interval_ms": "5000",
        "$__interval": "5s", "$__range": "15m",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text


def snapshot_sql(query, sequence):
    """Freeze the append-only scores table while the real stream keeps running."""
    if sequence is None:
        return query
    assert type(sequence) is int and sequence > 0, sequence
    return re.sub(r"\bFROM\s+scores\b", f"FROM (SELECT * FROM public.scores WHERE sequence <= {sequence}) AS scores", query, flags=re.IGNORECASE)


def query_target(target, state="__all", merchant="__all", *, from_ms=None, to_ms=None, snapshot_sequence=None):
    query = copy.deepcopy(target)
    query.update({"intervalMs": 5000, "maxDataPoints": 180})
    for field in ("rawSql", "expr"):
        if field in query:
            query[field] = substitute(query[field], state, merchant)
    if "rawSql" in query:
        query["rawSql"] = snapshot_sql(query["rawSql"], snapshot_sequence)
    now = int(time.time() * 1000)
    response = SESSION.post(GRAFANA + "/api/ds/query", json={
        "from": str(from_ms if from_ms is not None else now - 900000),
        "to": str(to_ms if to_ms is not None else now), "queries": [query],
    }, timeout=45)
    response.raise_for_status()
    result = response.json()["results"][query["refId"]]
    assert not result.get("error"), result
    assert result.get("status", 200) == 200, result
    frames = result.get("frames", [])
    assert frames, (query, result)
    return frames


def rows_of(frames):
    rows = []
    for frame in frames:
        fields = [field["name"] for field in frame["schema"]["fields"]]
        values = frame.get("data", {}).get("values", [])
        rows.extend(dict(zip(fields, row)) for row in zip(*values))
    return rows


def sql_query(query, **kwargs):
    return rows_of(query_target({"refId": "A", "datasource": {"type": "postgres", "uid": "postgres"},
                                "rawSql": query, "format": "table", "rawQuery": True}, **kwargs))


def alert_states():
    data = grafana_get("/api/prometheus/grafana/api/v1/rules")
    return {rule["name"]: rule["state"]
            for group in data["data"]["groups"] for rule in group["rules"]}


def wait_alert(name, expected, timeout):
    def probe():
        states = alert_states()
        return states if states.get(name) == expected else None
    result = wait_for(probe, f"{name} becomes {expected}", timeout)
    evidence = {"checked_at": datetime.now(timezone.utc).isoformat(), "states": result}
    safe = name.lower().replace(" ", "-")
    atomic_json(storage_root() / "evidence" / f"alert-{safe}-{expected}.json", evidence)
    print(json.dumps(evidence, indent=2), flush=True)


def check_dashboards():
    report = []
    for uid in ("hw5-scoring", "hw5-writer", "hw5-system", "hw5-business"):
        dashboard = grafana_get(f"/api/dashboards/uid/{uid}")["dashboard"]
        for panel in dashboard["panels"]:
            for target in panel.get("targets", []):
                if "expr" in target:
                    values = prom(substitute(target["expr"]))
                    assert values, (uid, panel["title"], target)
                    assert any(math.isfinite(float(item["value"][1])) for item in values), (uid, panel["title"], values)
                frames = query_target(target)
                count = len(rows_of(frames))
                # A model is allowed to flag no rows. An empty exposure panel
                # is then a valid business result, rather than a query failure.
                empty_is_valid = uid == "hw5-business" and panel["id"] == 8
                assert count > 0 or empty_is_valid, (uid, panel["title"], frames)
                report.append({"dashboard": uid, "panel": panel["title"], "ref": target["refId"], "rows": count})
        print(f"Verified {uid}: {len(dashboard['panels'])} panels", flush=True)
    return report


def check_business():
    dashboard = grafana_get("/api/dashboards/uid/hw5-business")["dashboard"]
    panels = {panel["id"]: panel for panel in dashboard["panels"]}
    # One serial writer inserts monotonically increasing sequences. Restrict
    # every comparison to its committed prefix so continued traffic cannot make
    # separately executed dashboard and reference queries disagree.
    snapshot = sql_query("SELECT max(sequence) AS sequence, count(*) AS transactions FROM scores")[0]
    sequence = int(snapshot["sequence"])
    to_ms = int(time.time()) * 1000
    from_ms = to_ms - 900000
    window = {"from_ms": from_ms, "to_ms": to_ms, "snapshot_sequence": sequence}
    dimensions = sql_query("""
        SELECT coalesce(nullif(us_state,''),'unknown') AS us_state,
               coalesce(nullif(merch,''),'unknown') AS merch, count(*) AS transactions
        FROM scores WHERE $__timeFilter(processed_at)
        GROUP BY 1, 2 ORDER BY transactions DESC, us_state, merch LIMIT 20
    """, **window)
    assert dimensions, "No recently processed rows available to exercise business filters"
    state, merchant = dimensions[0]["us_state"], dimensions[0]["merch"]
    selections = [("__all", "__all"), (state, "__all"), ("__all", merchant), (state, merchant)]

    # Exercise provisioned variable SQL against the same real records, including
    # values containing apostrophes. Custom CSV inputs need no demo names.
    variables = {item["name"]: item for item in dashboard["templating"]["list"]}
    options_report = []
    for name in ("us_state", "merch"):
        actual = sql_query(variables[name]["query"], **window)
        expected = sql_query(f"""
            SELECT 'All' AS __text, '__all' AS __value
            UNION SELECT coalesce(nullif({name},''),'unknown') AS __text,
                         coalesce(nullif({name},''),'unknown') AS __value FROM scores
        """, **window)
        actual_pairs = [(row["__text"], row["__value"]) for row in actual]
        expected_pairs = {(row["__text"], row["__value"]) for row in expected}
        assert set(actual_pairs) == expected_pairs, (name, actual, expected)
        assert len(actual_pairs) == len(expected_pairs), (name, "duplicate variable options", actual)
        assert actual_pairs[0] == ("All", "__all"), (name, actual)
        options_report.append({"variable": name, "options": len(actual_pairs),
                               "quoted_values": sum("'" in value for _, value in actual_pairs)})
    quoted = sql_query("""
        SELECT coalesce(nullif(us_state,''),'unknown') AS us_state, merch, count(*) AS transactions
        FROM scores WHERE $__timeFilter(processed_at) AND position(chr(39) in merch) > 0
        GROUP BY 1, 2 ORDER BY transactions DESC LIMIT 1
    """, **window)
    quoted_merchant = quoted[0]["merch"] if quoted else "O'Brien's verification store"
    escaped = sql_query("SELECT ${merch:sqlstring} AS merchant", merchant=quoted_merchant, **window)
    assert escaped == [{"merchant": quoted_merchant}], escaped
    if quoted:
        selections.append((quoted[0]["us_state"], quoted_merchant))

    filters = """('__all' IN (${us_state:sqlstring}) OR coalesce(nullif(us_state,''),'unknown') IN (${us_state:sqlstring}))
        AND ('__all' IN (${merch:sqlstring}) OR coalesce(nullif(merch,''),'unknown') IN (${merch:sqlstring}))"""
    checks = []
    for state, merchant in dict.fromkeys(selections):
        selection = {"state": state, "merchant": merchant, **window}
        count = rows_of(query_target(panels[1]["targets"][0], **selection))[0]["value"]
        assert count > 0, selection
        expected_count = sql_query("SELECT count(*) AS value FROM scores WHERE $__timeFilter(processed_at) AND " + filters, **selection)[0]["value"]
        assert count == expected_count, (selection, count, expected_count)
        density = rows_of(query_target(panels[5]["targets"][0], **selection))
        area = sum(row["density"] * 0.05 for row in density)
        assert abs(area - 1) < 1e-10, (selection, area)

        # Rate times each clipped bucket's actual duration must recover the
        # exact selected transaction count, including partial edge buckets.
        tps_frames = query_target(panels[6]["targets"][0], **selection)
        tps_points = []
        for frame in tps_frames:
            fields = frame["schema"]["fields"]
            time_index = next(index for index, field in enumerate(fields) if field["type"] == "time")
            tps_index = next(index for index, field in enumerate(fields) if field["name"] == "tps")
            values = frame["data"]["values"]
            tps_points.extend(zip(values[time_index], values[tps_index]))
        tps_points.sort()
        expected_times = [max(bucket * 5000, from_ms)
                          for bucket in range(from_ms // 5000, (to_ms + 4999) // 5000)]
        assert [stamp for stamp, _ in tps_points] == expected_times, (selection, tps_points)
        integrated_count = 0.0
        for index, (stamp, tps) in enumerate(tps_points):
            assert tps is not None and math.isfinite(tps) and tps >= 0, (selection, stamp, tps)
            end = tps_points[index + 1][0] if index + 1 < len(tps_points) else to_ms
            integrated_count += tps * (end - stamp) / 1000
        assert math.isclose(integrated_count, count, rel_tol=1e-10, abs_tol=1e-8), (selection, integrated_count, count)

        table = rows_of(query_target(panels[11]["targets"][0], **selection))
        assert table, selection
        assert all(state == "__all" or (row["us_state"] or "unknown") == state for row in table), table
        assert all(merchant == "__all" or (row["merch"] or "unknown") == merchant for row in table), table
        # Category denominators must cover the global newest 1,000 first, then filters.
        actual_counts = {row["category"]: row for row in rows_of(query_target(panels[10]["targets"][0], **selection))}
        actual_fractions = {row["category"]: row["fraud_fraction"]
                            for row in rows_of(query_target(panels[7]["targets"][0], **selection))}
        expected_categories = sql_query("""
            WITH ranked AS (
                SELECT cat_id, fraud_flag, us_state, merch,
                       row_number() OVER (ORDER BY sequence DESC) AS recency FROM scores
            )
            SELECT coalesce(nullif(cat_id,''),'unknown') AS category,
                   count(*) AS transactions, sum(fraud_flag) AS flagged
            FROM ranked WHERE recency <= 1000 AND """ + filters + " GROUP BY 1", **selection)
        expected_counts = {row["category"]: row for row in expected_categories}
        assert actual_counts == expected_counts, (selection, actual_counts, expected_counts)
        assert actual_fractions.keys() == expected_counts.keys(), (selection, actual_fractions, expected_counts)
        for category, expected in expected_counts.items():
            expected_fraction = expected["flagged"] / expected["transactions"]
            assert math.isclose(actual_fractions[category], expected_fraction, rel_tol=1e-12, abs_tol=1e-12), (selection, category, actual_fractions[category], expected_fraction)
        category_rows = sum(row["transactions"] for row in expected_counts.values())
        if state == merchant == "__all":
            assert category_rows == min(1000, snapshot["transactions"]), (category_rows, snapshot)
        checks.append({"state": state, "merchant": merchant, "transactions": count,
                       "density_area": area, "tps_integrated_count": integrated_count,
                       "category_window_rows": category_rows, "categories": expected_categories})
    readonly = sql_query("SELECT current_user AS username, current_setting('default_transaction_read_only') AS readonly, has_table_privilege(current_user,'scores','INSERT') AS can_insert")
    assert readonly == [{"username": "grafana_reader", "readonly": "on", "can_insert": False}], readonly
    return {"snapshot_sequence": sequence, "snapshot_transactions": snapshot["transactions"],
            "from_ms": from_ms, "to_ms": to_ms, "filters": checks,
            "variable_options": options_report, "quoted_merchant_roundtrip": quoted_merchant,
            "quoted_merchant_has_live_rows": bool(quoted), "database_role": readonly}


def verify(skip_retrain=False):
    report = {"started_at": datetime.now(timezone.utc).isoformat()}
    destination = storage_root() / "evidence" / "monitoring.json"
    try:
        wait_for(lambda: scalar('count(up == 1)') == 4, "all four Prometheus targets up")
        report["targets"] = prom("up")
        before = {metric: scalar(metric) for metric in ("fw_transactions_total", "fw_scores_written_total")}
        wait_for(lambda: all(scalar(metric) >= value + 30 for metric, value in before.items()), "live counters grow by 30")
        report["live_counters"] = {metric: {"before": value, "after": scalar(metric)} for metric, value in before.items()}
        wait_for(lambda: scalar("sum(rate(fw_transactions_total[30s]))") > 0, "live rates available")
        report["model_before"] = ModelRegistry().current()
        if not skip_retrain:
            report["retraining"] = run_retraining(advance=True)
            after = report["retraining"]["active_manifest"]
            assert after["version"] != report["model_before"]["version"], "Quality gate did not publish; inspect decision"
            version = after["version"]
            wait_for(lambda: prom(f'fw_model_activations_total{{version="{version}"}}'), "scorer loads new model")
            annotations = prom('(fw_model_activated_timestamp_seconds > (time() - 30)) * 1000')
            assert any(item["metric"]["version"] == version for item in annotations), annotations
            assert all(float(item["value"][1]) > 1e12 for item in annotations), annotations
            report["activation_annotation"] = annotations
            def stored_new_version():
                with psycopg2.connect(connect_timeout=10) as connection, connection.cursor() as cursor:
                    cursor.execute("SELECT count(*) FROM scores WHERE model_version=%s", (version,))
                    return cursor.fetchone()[0] >= 10
            wait_for(stored_new_version, "new version persisted by live writer")
            report["model_after"] = after
        report["dashboard_queries"] = check_dashboards()
        report["business"] = check_business()
        rules = grafana_get("/api/v1/provisioning/alert-rules")
        assert {rule["uid"] for rule in rules} >= {"hw5-high-fraud-ratio", "hw5-stream-stalled"}, rules
        report["alert_rules"] = [{"uid": rule["uid"], "title": rule["title"]} for rule in rules]
        report["alert_states"] = alert_states()
        report["status"] = "passed"
    except Exception as error:
        report["status"] = "failed"
        report["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        atomic_json(destination, report)
    print(json.dumps({"status": "passed", "evidence": str(destination),
                      "queries_checked": len(report["dashboard_queries"])}, indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-retrain", action="store_true")
    parser.add_argument("--alert-name")
    parser.add_argument("--alert-state", choices=["firing", "inactive", "pending"])
    parser.add_argument("--timeout", type=int, default=180)
    args = parser.parse_args()
    if args.alert_name:
        parser.error("--alert-state required") if not args.alert_state else wait_alert(args.alert_name, args.alert_state, args.timeout)
    else:
        verify(args.skip_retrain)
