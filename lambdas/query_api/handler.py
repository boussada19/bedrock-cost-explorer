"""
Query API Lambda — dashboard read layer.
"""

import json
import logging
import os
import traceback
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import boto3
from boto3.dynamodb.conditions import Attr

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

dynamodb = boto3.resource("dynamodb")
EVENTS_TABLE = os.environ["EVENTS_TABLE"]
RECONCILIATION_TABLE = os.environ["RECONCILIATION_TABLE"]

events_table = dynamodb.Table(EVENTS_TABLE)
recon_table = dynamodb.Table(RECONCILIATION_TABLE)

DEFAULT_WINDOW_HOURS = 24


def parse_time_params(params: Dict) -> Tuple[str, str]:
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(hours=DEFAULT_WINDOW_HOURS)
    if "end" in params:
        try:
            end_dt = datetime.fromisoformat(params["end"].replace("Z", "+00:00"))
        except ValueError:
            pass
    if "start" in params:
        try:
            start_dt = datetime.fromisoformat(params["start"].replace("Z", "+00:00"))
        except ValueError:
            pass
    return start_dt.isoformat(), end_dt.isoformat()


def scan_events(start_ts: str, end_ts: str) -> List[Dict]:
    filter_expr = Attr("timestamp").between(start_ts, end_ts)
    items = []
    scan_kwargs = {
        "FilterExpression": filter_expr,
        "ProjectionExpression": (
            "event_id, model_id, agent_id, user_id, application_id, "
            "input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, "
            "computed_cost_usd, #ts, #src, #st"
        ),
        "ExpressionAttributeNames": {
            "#src": "source",
            "#ts": "timestamp",
            "#st": "status",
        },
    }
    resp = events_table.scan(**scan_kwargs)
    items.extend(resp.get("Items", []))
    while resp.get("LastEvaluatedKey"):
        resp = events_table.scan(**scan_kwargs, ExclusiveStartKey=resp["LastEvaluatedKey"])
        items.extend(resp.get("Items", []))
    return items


def aggregate_by_dimension(items: List[Dict], dimension: str) -> List[Dict]:
    groups: Dict[str, Dict] = {}
    for item in items:
        key = item.get(dimension, "NULL")
        if not key or key == "NULL":
            key = "(unattributed)"
        if key not in groups:
            groups[key] = {
                dimension: key,
                "total_cost_usd": Decimal("0"),
                "input_tokens": 0,
                "output_tokens": 0,
                "event_count": 0,
                "error_count": 0,
            }
        g = groups[key]
        g["event_count"] += 1
        g["input_tokens"] += int(item.get("input_tokens", 0))
        g["output_tokens"] += int(item.get("output_tokens", 0))
        cost = item.get("computed_cost_usd")
        if cost is not None:
            g["total_cost_usd"] += Decimal(str(cost))
        if item.get("status") == "error":
            g["error_count"] += 1
    result = sorted(groups.values(), key=lambda x: x["total_cost_usd"], reverse=True)
    return [_serialize(r) for r in result]


def build_timeseries(items: List[Dict], granularity: str) -> List[Dict]:
    buckets: Dict[str, Dict] = {}
    for item in items:
        ts = item.get("timestamp", "")
        if not ts:
            continue
        bucket_key = ts[:10] if granularity == "day" else ts[:13]
        if bucket_key not in buckets:
            buckets[bucket_key] = {
                "period": bucket_key,
                "total_cost_usd": Decimal("0"),
                "input_tokens": 0,
                "output_tokens": 0,
                "event_count": 0,
            }
        b = buckets[bucket_key]
        b["event_count"] += 1
        b["input_tokens"] += int(item.get("input_tokens", 0))
        b["output_tokens"] += int(item.get("output_tokens", 0))
        cost = item.get("computed_cost_usd")
        if cost is not None:
            b["total_cost_usd"] += Decimal(str(cost))
    return [_serialize(v) for v in sorted(buckets.values(), key=lambda x: x["period"])]


def _serialize(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serialize(i) for i in obj]
    return obj


def handle_summary(params: Dict) -> Dict:
    start_ts, end_ts = parse_time_params(params)
    items = scan_events(start_ts, end_ts)
    total_cost = sum(Decimal(str(i.get("computed_cost_usd", 0) or 0)) for i in items)
    total_input = sum(int(i.get("input_tokens", 0)) for i in items)
    total_output = sum(int(i.get("output_tokens", 0)) for i in items)
    error_count = sum(1 for i in items if i.get("status") == "error")
    wrapper_count = sum(1 for i in items if i.get("source") == "wrapper")
    return {
        "window": {"start": start_ts, "end": end_ts},
        "total_cost_usd": float(total_cost),
        "total_input_tokens": total_input,
        "total_output_tokens": total_output,
        "total_events": len(items),
        "error_count": error_count,
        "wrapper_coverage_pct": round(
            wrapper_count / len(items) * 100 if items else 100.0, 2
        ),
    }


def handle_by_dimension(params: Dict, dimension: str) -> Dict:
    start_ts, end_ts = parse_time_params(params)
    items = scan_events(start_ts, end_ts)
    return {
        "window": {"start": start_ts, "end": end_ts},
        "dimension": dimension,
        "items": aggregate_by_dimension(items, dimension),
    }


def handle_timeseries(params: Dict) -> Dict:
    start_ts, end_ts = parse_time_params(params)
    granularity = params.get("granularity", "hour")
    if granularity not in ("hour", "day"):
        granularity = "hour"
    items = scan_events(start_ts, end_ts)
    return {
        "window": {"start": start_ts, "end": end_ts},
        "granularity": granularity,
        "series": build_timeseries(items, granularity),
    }


def handle_reconciliation(params: Dict) -> Dict:
    limit = int(params.get("limit", "30"))
    resp = recon_table.scan(FilterExpression=Attr("SK").eq("SUMMARY"))
    runs = sorted(
        resp.get("Items", []),
        key=lambda x: x.get("run_date", ""),
        reverse=True,
    )[:limit]
    return {"runs": _serialize(runs)}


def handle_coverage(params: Dict) -> Dict:
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=7)
    items = scan_events(start_dt.isoformat(), end_dt.isoformat())
    wrapper_count = sum(1 for i in items if i.get("source") == "wrapper")
    backfill_count = sum(1 for i in items if i.get("source") == "cloudwatch_backfill")
    total = len(items)
    return {
        "window_days": 7,
        "total_events": total,
        "wrapper_events": wrapper_count,
        "backfill_events": backfill_count,
        "coverage_pct": round(wrapper_count / total * 100 if total > 0 else 100.0, 2),
    }


ROUTE_MAP = {
    "/query/summary":        handle_summary,
    "/query/by-agent":       lambda p: handle_by_dimension(p, "agent_id"),
    "/query/by-user":        lambda p: handle_by_dimension(p, "user_id"),
    "/query/by-app":         lambda p: handle_by_dimension(p, "application_id"),
    "/query/by-model":       lambda p: handle_by_dimension(p, "model_id"),
    "/query/timeseries":     handle_timeseries,
    "/query/reconciliation": handle_reconciliation,
    "/query/coverage":       handle_coverage,
}


def lambda_handler(event: dict, context: Any) -> dict:
    path = event.get("path", "")
    params = event.get("queryStringParameters") or {}

    logger.info("Query request: path=%s params=%s", path, params)

    # Normalise proxy path
    if "/query/" in path:
        normalized_path = "/query/" + path.split("/query/")[-1].lstrip("/")
    else:
        normalized_path = path

    handler_fn = ROUTE_MAP.get(normalized_path)
    if handler_fn is None:
        return _response(404, {
            "error": f"Unknown endpoint: {path}",
            "normalized": normalized_path,
            "available_endpoints": sorted(ROUTE_MAP.keys()),
        })

    try:
        result = handler_fn(params)
        return _response(200, result)
    except Exception as exc:
        # Log full traceback so it appears in CloudWatch
        tb = traceback.format_exc()
        logger.error("Query error on %s: %s\n%s", path, exc, tb)
        return _response(500, {
            "error": "Internal query error",
            "detail": str(exc),
            "path": normalized_path,
        })


def _response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body, default=str),
    }