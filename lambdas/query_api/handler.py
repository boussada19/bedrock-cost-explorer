"""
Query API Lambda — dashboard read layer.

Serves the React dashboard via API Gateway GET endpoints.
All aggregations run against the DynamoDB events table using GSIs.

Endpoints:
  GET /query/summary?start=<ISO>&end=<ISO>           — global cost/token totals
  GET /query/by-agent?start=&end=                    — cost breakdown by agent
  GET /query/by-user?start=&end=                     — cost breakdown by user
  GET /query/by-app?start=&end=                      — cost breakdown by application
  GET /query/by-model?start=&end=                    — cost breakdown by model
  GET /query/timeseries?start=&end=&granularity=hour  — cost over time (hour/day)
  GET /query/reconciliation?limit=30                 — recent reconciliation runs
  GET /query/coverage                                — wrapper coverage metrics

Design note: At low-medium scale (thousands/day), scanning with filters is
acceptable. The migration path to Athena/ClickHouse requires only swapping
the query functions here — the API surface and response schema stay identical.
"""

import json
import logging
import os
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key, Attr

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

dynamodb = boto3.resource("dynamodb")
EVENTS_TABLE = os.environ["EVENTS_TABLE"]
RECONCILIATION_TABLE = os.environ["RECONCILIATION_TABLE"]

events_table = dynamodb.Table(EVENTS_TABLE)
recon_table = dynamodb.Table(RECONCILIATION_TABLE)

# Default time window: last 24 hours
DEFAULT_WINDOW_HOURS = 24


def parse_time_params(params: dict) -> tuple[str, str]:
    """Parse start/end from query params, defaulting to last 24h."""
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


def scan_events(start_ts: str, end_ts: str, extra_filter=None) -> list[dict]:
    """
    Scan events table for the given time window.
    At scale, replace this with a date-range GSI or Athena query.
    """
    filter_expr = Attr("timestamp").between(start_ts, end_ts)
    if extra_filter is not None:
        filter_expr = filter_expr & extra_filter

    items = []
    resp = events_table.scan(
        FilterExpression=filter_expr,
        ProjectionExpression=(
            "event_id, model_id, agent_id, user_id, application_id, "
            "input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, "
            "computed_cost_usd, timestamp, #src, status"
        ),
        ExpressionAttributeNames={"#src": "source"},
    )
    items.extend(resp.get("Items", []))
    while resp.get("LastEvaluatedKey"):
        resp = events_table.scan(
            FilterExpression=filter_expr,
            ProjectionExpression=(
                "event_id, model_id, agent_id, user_id, application_id, "
                "input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, "
                "computed_cost_usd, timestamp, #src, status"
            ),
            ExpressionAttributeNames={"#src": "source"},
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        items.extend(resp.get("Items", []))

    return items


def aggregate_by_dimension(items: list[dict], dimension: str) -> list[dict]:
    """
    Group items by a dimension and compute cost/token totals per group.
    Filters out 'NULL' sentinel values.
    """
    groups: dict[str, dict] = {}

    for item in items:
        key = item.get(dimension, "NULL")
        if key == "NULL" or not key:
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

    # Sort by cost descending
    result = sorted(
        groups.values(),
        key=lambda x: x["total_cost_usd"],
        reverse=True,
    )
    return [_serialize(r) for r in result]


def build_timeseries(items: list[dict], granularity: str) -> list[dict]:
    """
    Build a time-series of cost and token counts at hour or day granularity.
    """
    buckets: dict[str, dict] = {}

    for item in items:
        ts = item.get("timestamp", "")
        if not ts:
            continue

        if granularity == "day":
            bucket_key = ts[:10]  # YYYY-MM-DD
        else:
            bucket_key = ts[:13]  # YYYY-MM-DDTHH

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

    # Sort chronologically
    return [_serialize(v) for v in sorted(buckets.values(), key=lambda x: x["period"])]


def _serialize(obj: Any) -> Any:
    """Recursively convert Decimal to float for JSON serialization."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serialize(i) for i in obj]
    return obj


def handle_summary(params: dict) -> dict:
    start_ts, end_ts = parse_time_params(params)
    items = scan_events(start_ts, end_ts)

    total_cost = sum(
        Decimal(str(i.get("computed_cost_usd", 0) or 0)) for i in items
    )
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


def handle_by_dimension(params: dict, dimension: str) -> dict:
    start_ts, end_ts = parse_time_params(params)
    items = scan_events(start_ts, end_ts)
    breakdown = aggregate_by_dimension(items, dimension)
    return {
        "window": {"start": start_ts, "end": end_ts},
        "dimension": dimension,
        "items": breakdown,
    }


def handle_timeseries(params: dict) -> dict:
    start_ts, end_ts = parse_time_params(params)
    granularity = params.get("granularity", "hour")
    if granularity not in ("hour", "day"):
        granularity = "hour"
    items = scan_events(start_ts, end_ts)
    series = build_timeseries(items, granularity)
    return {
        "window": {"start": start_ts, "end": end_ts},
        "granularity": granularity,
        "series": series,
    }


def handle_reconciliation(params: dict) -> dict:
    limit = int(params.get("limit", "30"))
    resp = recon_table.scan(
        FilterExpression=Attr("SK").eq("SUMMARY"),
        Limit=limit,
    )
    runs = sorted(
        resp.get("Items", []),
        key=lambda x: x.get("run_date", ""),
        reverse=True,
    )[:limit]
    return {"runs": _serialize(runs)}


def handle_coverage(params: dict) -> dict:
    """Return wrapper vs backfill event counts for the last 7 days."""
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=7)
    start_ts = start_dt.isoformat()
    end_ts = end_dt.isoformat()

    items = scan_events(start_ts, end_ts)
    wrapper_count = sum(1 for i in items if i.get("source") == "wrapper")
    backfill_count = sum(
        1 for i in items if i.get("source") == "cloudwatch_backfill"
    )
    total = len(items)

    return {
        "window_days": 7,
        "total_events": total,
        "wrapper_events": wrapper_count,
        "backfill_events": backfill_count,
        "coverage_pct": round(
            wrapper_count / total * 100 if total > 0 else 100.0, 2
        ),
    }


ROUTE_MAP = {
    "/query/summary": handle_summary,
    "/query/by-agent": lambda p: handle_by_dimension(p, "agent_id"),
    "/query/by-user": lambda p: handle_by_dimension(p, "user_id"),
    "/query/by-app": lambda p: handle_by_dimension(p, "application_id"),
    "/query/by-model": lambda p: handle_by_dimension(p, "model_id"),
    "/query/timeseries": handle_timeseries,
    "/query/reconciliation": handle_reconciliation,
    "/query/coverage": handle_coverage,
}


def lambda_handler(event: dict, context: Any) -> dict:
    path = event.get("path", "")
    params = event.get("queryStringParameters") or {}

    # Normalise proxy path (API GW may include /query/{proxy+})
    normalized_path = "/query/" + path.split("/query/")[-1].lstrip("/")

    handler_fn = ROUTE_MAP.get(normalized_path)
    if handler_fn is None:
        available = sorted(ROUTE_MAP.keys())
        return _response(
            404,
            {
                "error": f"Unknown endpoint: {path}",
                "available_endpoints": available,
            },
        )

    try:
        result = handler_fn(params)
        return _response(200, result)
    except Exception as exc:
        logger.exception("Query error on %s: %s", path, exc)
        return _response(500, {"error": "Internal query error"})


def _response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body, default=str),
    }
