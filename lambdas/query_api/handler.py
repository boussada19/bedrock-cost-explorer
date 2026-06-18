"""
Query API Lambda — MSP multi-tenant read layer.

All existing endpoints are backward-compatible. Tenant-scoped queries
are activated by passing ?tenantId=<client-id> as a query string parameter.

When tenantId is supplied:
  - summary, timeseries, by-* endpoints use a .query() against
    SourceTimestampIndex (GSI on `source` = tenantId) instead of a
    full-table scan.  This is O(tenant-events) not O(all-events).
  - coverage and reconciliation continue to aggregate across all tenants
    (those are MSP-level metrics).

Endpoints:
  GET /query/summary?[start=&end=][&tenantId=]
  GET /query/by-agent?...
  GET /query/by-user?...
  GET /query/by-app?...
  GET /query/by-model?...
  GET /query/timeseries?...&granularity=hour|day
  GET /query/reconciliation?limit=30
  GET /query/coverage
  GET /query/tenants                  ← NEW: list known tenant IDs
  GET /query/tenant-summary?tenantId= ← NEW: full MSP report for one tenant
"""

import json
import logging
import os
import traceback
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import boto3
from boto3.dynamodb.conditions import Attr, Key

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

dynamodb      = boto3.resource("dynamodb")
EVENTS_TABLE  = os.environ["EVENTS_TABLE"]
RECON_TABLE   = os.environ["RECONCILIATION_TABLE"]
TENANT_INDEX  = os.environ.get("TENANT_INDEX", "SourceTimestampIndex")

events_table = dynamodb.Table(EVENTS_TABLE)
recon_table  = dynamodb.Table(RECON_TABLE)

DEFAULT_WINDOW_HOURS = 24

# ─────────────────────────────────────────────────────────────────
# Latency benchmarks (ms, p50) keyed by model_id prefix.
# Used when response_latency_ms is absent from the record.
# ─────────────────────────────────────────────────────────────────
LATENCY_BENCHMARKS: Dict[str, Dict[str, float]] = {
    "anthropic.claude-sonnet": {"p50": 1200, "p95": 3800, "p99": 7200},
    "anthropic.claude-haiku":  {"p50":  380, "p95":  920, "p99": 1800},
    "anthropic.claude-opus":   {"p50": 3100, "p95": 9200, "p99": 18000},
    "meta.llama3-70b":         {"p50": 1600, "p95": 4200, "p99": 8500},
    "meta.llama3-8b":          {"p50":  520, "p95": 1400, "p99": 2800},
    "mistral":                 {"p50": 1050, "p95": 3100, "p99": 6200},
    "amazon.titan":            {"p50":  900, "p95": 2700, "p99": 5400},
}
LATENCY_FALLBACK = {"p50": 1100, "p95": 3500, "p99": 7000}


# ─────────────────────────────────────────────────────────────────
# Time helpers
# ─────────────────────────────────────────────────────────────────

def parse_time_params(params: Dict) -> Tuple[str, str]:
    end_dt   = datetime.now(timezone.utc)
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


# ─────────────────────────────────────────────────────────────────
# Data retrieval — two paths: full scan vs tenant GSI query
# ─────────────────────────────────────────────────────────────────

PROJECTION = (
    "event_id, model_id, agent_id, user_id, application_id, "
    "input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, "
    "computed_cost_usd, latency_ms, #ts, #src, #st"
)
EXPR_NAMES = {"#src": "source", "#ts": "timestamp", "#st": "status"}


def scan_events(start_ts: str, end_ts: str) -> List[Dict]:
    """Full-table scan filtered to a time window. Used for All-Tenants view."""
    kwargs: Dict = {
        "FilterExpression": Attr("timestamp").between(start_ts, end_ts),
        "ProjectionExpression": PROJECTION,
        "ExpressionAttributeNames": EXPR_NAMES,
    }
    items: List[Dict] = []
    resp = events_table.scan(**kwargs)
    items.extend(resp.get("Items", []))
    while resp.get("LastEvaluatedKey"):
        resp = events_table.scan(**kwargs, ExclusiveStartKey=resp["LastEvaluatedKey"])
        items.extend(resp.get("Items", []))
    logger.info("scan_events: %d items in [%s, %s]", len(items), start_ts, end_ts)
    return items


def query_tenant_events(tenant_id: str, start_ts: str, end_ts: str) -> List[Dict]:
    """
    Efficient GSI query for a single tenant.

    Uses SourceTimestampIndex:
      KeyConditionExpression: source = :tid AND timestamp BETWEEN :s AND :e

    This is O(tenant-events) and avoids scanning the full table.
    """
    kwargs: Dict = {
        "IndexName": TENANT_INDEX,
        "KeyConditionExpression": (
            Key("source").eq(tenant_id)
            & Key("timestamp").between(start_ts, end_ts)
        ),
        "ProjectionExpression": PROJECTION,
        "ExpressionAttributeNames": EXPR_NAMES,
    }
    items: List[Dict] = []
    resp = events_table.query(**kwargs)
    items.extend(resp.get("Items", []))
    while resp.get("LastEvaluatedKey"):
        resp = events_table.query(**kwargs, ExclusiveStartKey=resp["LastEvaluatedKey"])
        items.extend(resp.get("Items", []))
    logger.info(
        "query_tenant_events: %d items for tenant=%s in [%s, %s]",
        len(items), tenant_id, start_ts, end_ts,
    )
    return items


def get_events(params: Dict, start_ts: str, end_ts: str) -> List[Dict]:
    """Router: use GSI query when tenantId is present, scan otherwise."""
    tenant_id = params.get("tenantId", "").strip()
    if tenant_id:
        return query_tenant_events(tenant_id, start_ts, end_ts)
    return scan_events(start_ts, end_ts)


# ─────────────────────────────────────────────────────────────────
# Tenant authorization — derive scope from the VERIFIED Cognito JWT.
#
# API Gateway's Cognito authorizer validates the token signature and
# expiry, then forwards the claims under requestContext.authorizer.claims.
# We read custom:tenant_id from there. A client can NOT override this by
# passing ?tenantId= — server-side scope always wins.
# ─────────────────────────────────────────────────────────────────

def resolve_tenant_scope(event: dict, params: Dict) -> Tuple[Optional[str], bool]:
    """
    Returns (effective_tenant_id, is_admin).

      - Admin (custom:tenant_id == "*"): may pass ?tenantId= to scope to any
        client, or omit it to see all tenants. effective = the param or None.
      - Client (custom:tenant_id == "client-x"): ALWAYS scoped to their own
        tenant regardless of any query param they try to send.

    If no claims are present (e.g. local testing without authorizer),
    falls back to the query param so existing tooling keeps working.
    """
    claims = (
        event.get("requestContext", {})
             .get("authorizer", {})
             .get("claims", {})
    )
    token_tenant = claims.get("custom:tenant_id")

    # No authenticated context (local/dev) → honor the param as before
    if token_tenant is None:
        return (params.get("tenantId", "").strip() or None, False)

    # Admin: "*" means all tenants; respect optional ?tenantId= filter
    if token_tenant == "*":
        requested = params.get("tenantId", "").strip()
        return (requested or None, True)

    # Client: hard-locked to their own tenant
    return (token_tenant, False)


# ─────────────────────────────────────────────────────────────────
# Aggregation helpers
# ─────────────────────────────────────────────────────────────────

def _latency_for_model(model_id: str) -> Dict[str, float]:
    for prefix, bench in LATENCY_BENCHMARKS.items():
        if model_id.startswith(prefix):
            return bench
    return LATENCY_FALLBACK


def aggregate_by_dimension(items: List[Dict], dimension: str) -> List[Dict]:
    groups: Dict[str, Dict] = {}
    for item in items:
        key = item.get(dimension) or "(unattributed)"
        if key == "NULL":
            key = "(unattributed)"
        if key not in groups:
            groups[key] = {
                dimension: key,
                "total_cost_usd": Decimal("0"),
                "input_tokens":   0,
                "output_tokens":  0,
                "event_count":    0,
                "error_count":    0,
            }
        g = groups[key]
        g["event_count"]  += 1
        g["input_tokens"]  += int(item.get("input_tokens", 0))
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
        key = ts[:10] if granularity == "day" else ts[:13]
        if key not in buckets:
            buckets[key] = {
                "period": key, "total_cost_usd": Decimal("0"),
                "input_tokens": 0, "output_tokens": 0, "event_count": 0,
            }
        b = buckets[key]
        b["event_count"]  += 1
        b["input_tokens"]  += int(item.get("input_tokens", 0))
        b["output_tokens"] += int(item.get("output_tokens", 0))
        cost = item.get("computed_cost_usd")
        if cost is not None:
            b["total_cost_usd"] += Decimal(str(cost))
    return [_serialize(v) for v in sorted(buckets.values(), key=lambda x: x["period"])]


def compute_health(items: List[Dict]) -> Dict:
    """
    Enterprise health score: success rate weighted by call count.
    Also computes latency breakdown per model using recorded
    latency_ms when present, falling back to benchmarks.
    """
    total  = len(items)
    if total == 0:
        return {
            "health_score": 100.0,
            "success_count": 0, "error_count": 0, "throttled_count": 0,
            "total_events": 0, "latency_by_model": [],
        }

    status_counts: Dict[str, int] = {}
    latency_data:  Dict[str, List[float]] = {}

    for item in items:
        st = item.get("status", "unknown")
        status_counts[st] = status_counts.get(st, 0) + 1
        mid = item.get("model_id", "unknown")
        recorded = item.get("latency_ms")
        lat = float(recorded) if recorded is not None else None
        if mid not in latency_data:
            latency_data[mid] = []
        if lat is not None:
            latency_data[mid].append(lat)

    success   = status_counts.get("success", 0)
    errors    = status_counts.get("error", 0)
    throttled = status_counts.get("throttled", 0)
    health    = round((success / total) * 100, 2)

    latency_by_model = []
    for mid, recorded_lats in latency_data.items():
        bench = _latency_for_model(mid)
        if recorded_lats:
            sorted_lats = sorted(recorded_lats)
            n = len(sorted_lats)
            p50 = sorted_lats[int(n * 0.50)]
            p95 = sorted_lats[min(int(n * 0.95), n - 1)]
            p99 = sorted_lats[min(int(n * 0.99), n - 1)]
            source = "measured"
        else:
            p50, p95, p99 = bench["p50"], bench["p95"], bench["p99"]
            source = "benchmark"
        latency_by_model.append({
            "model_id": mid,
            "p50_ms": round(p50),
            "p95_ms": round(p95),
            "p99_ms": round(p99),
            "source": source,
            "sample_count": len(recorded_lats),
        })

    latency_by_model.sort(key=lambda x: x["p50_ms"])

    return {
        "health_score":    health,
        "success_count":   success,
        "error_count":     errors,
        "throttled_count": throttled,
        "total_events":    total,
        "latency_by_model": latency_by_model,
    }


def _serialize(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serialize(i) for i in obj]
    return obj


# ─────────────────────────────────────────────────────────────────
# Route handlers
# ─────────────────────────────────────────────────────────────────

def handle_summary(params: Dict) -> Dict:
    start_ts, end_ts = parse_time_params(params)
    tenant_id = params.get("tenantId", "").strip()
    items = get_events(params, start_ts, end_ts)

    total_cost   = sum(Decimal(str(i.get("computed_cost_usd", 0) or 0)) for i in items)
    total_input  = sum(int(i.get("input_tokens", 0)) for i in items)
    total_output = sum(int(i.get("output_tokens", 0)) for i in items)
    error_count  = sum(1 for i in items if i.get("status") == "error")
    wrapper_count = sum(1 for i in items if i.get("source") == "wrapper")

    return {
        "window":              {"start": start_ts, "end": end_ts},
        "tenant_id":           tenant_id or None,
        "total_cost_usd":      float(total_cost),
        "total_input_tokens":  total_input,
        "total_output_tokens": total_output,
        "total_events":        len(items),
        "error_count":         error_count,
        "wrapper_coverage_pct": round(
            wrapper_count / len(items) * 100 if items else 100.0, 2
        ),
    }


def handle_by_dimension(params: Dict, dimension: str) -> Dict:
    start_ts, end_ts = parse_time_params(params)
    items = get_events(params, start_ts, end_ts)
    return {
        "window":    {"start": start_ts, "end": end_ts},
        "tenant_id": params.get("tenantId") or None,
        "dimension": dimension,
        "items":     aggregate_by_dimension(items, dimension),
    }


def handle_timeseries(params: Dict) -> Dict:
    start_ts, end_ts = parse_time_params(params)
    granularity = params.get("granularity", "hour")
    if granularity not in ("hour", "day"):
        granularity = "hour"
    items = get_events(params, start_ts, end_ts)
    return {
        "window":      {"start": start_ts, "end": end_ts},
        "tenant_id":   params.get("tenantId") or None,
        "granularity": granularity,
        "series":      build_timeseries(items, granularity),
    }


def handle_reconciliation(params: Dict) -> Dict:
    limit = int(params.get("limit", "30"))
    resp  = recon_table.scan(FilterExpression=Attr("SK").eq("SUMMARY"))
    runs  = sorted(
        resp.get("Items", []),
        key=lambda x: x.get("run_date", ""),
        reverse=True,
    )[:limit]
    return {"runs": _serialize(runs)}


def handle_coverage(params: Dict) -> Dict:
    end_dt   = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=7)
    items    = scan_events(start_dt.isoformat(), end_dt.isoformat())
    wrapper  = sum(1 for i in items if i.get("source") == "wrapper")
    backfill = sum(1 for i in items if i.get("source") == "cloudwatch_backfill")
    total    = len(items)
    return {
        "window_days":    7,
        "total_events":   total,
        "wrapper_events": wrapper,
        "backfill_events": backfill,
        "coverage_pct":   round(wrapper / total * 100 if total > 0 else 100.0, 2),
    }


def handle_tenants(params: Dict) -> Dict:
    """
    Return the list of distinct tenant IDs seen in the events table.
    Scans the SourceTimestampIndex with a projection on `source` only,
    then deduplicates.  Excludes internal sentinel values.
    """
    INTERNAL = {"wrapper", "cloudwatch_backfill", "NULL"}
    seen: set = set()
    kwargs: Dict = {
        "IndexName":             TENANT_INDEX,
        "ProjectionExpression":  "#src",
        "ExpressionAttributeNames": {"#src": "source"},
    }
    resp = events_table.scan(**kwargs)
    for item in resp.get("Items", []):
        s = item.get("source", "")
        if s and s not in INTERNAL:
            seen.add(s)
    while resp.get("LastEvaluatedKey"):
        resp = events_table.scan(**kwargs, ExclusiveStartKey=resp["LastEvaluatedKey"])
        for item in resp.get("Items", []):
            s = item.get("source", "")
            if s and s not in INTERNAL:
                seen.add(s)
    return {"tenants": sorted(seen), "count": len(seen)}


def handle_tenant_summary(params: Dict) -> Dict:
    """
    Full MSP report for a single tenant: cost, tokens, health score,
    latency breakdown, and model breakdown — all in one call.
    Designed for the client-facing card in the Atomic Computing dashboard.
    """
    tenant_id = params.get("tenantId", "").strip()
    if not tenant_id:
        return _response_body_error("tenantId is required for /query/tenant-summary")

    start_ts, end_ts = parse_time_params(params)
    items = query_tenant_events(tenant_id, start_ts, end_ts)

    total_cost   = float(sum(Decimal(str(i.get("computed_cost_usd", 0) or 0)) for i in items))
    total_input  = sum(int(i.get("input_tokens", 0)) for i in items)
    total_output = sum(int(i.get("output_tokens", 0)) for i in items)
    health       = compute_health(items)
    by_model     = aggregate_by_dimension(items, "model_id")
    series       = build_timeseries(items, "day")

    return {
        "tenant_id":           tenant_id,
        "window":              {"start": start_ts, "end": end_ts},
        "total_cost_usd":      total_cost,
        "total_input_tokens":  total_input,
        "total_output_tokens": total_output,
        "total_events":        len(items),
        "health":              health,
        "by_model":            by_model,
        "daily_series":        series,
    }


def _response_body_error(msg: str) -> Dict:
    raise ValueError(msg)


# ─────────────────────────────────────────────────────────────────
# Router
# ─────────────────────────────────────────────────────────────────

ROUTE_MAP = {
    "/query/summary":        handle_summary,
    "/query/by-agent":       lambda p: handle_by_dimension(p, "agent_id"),
    "/query/by-user":        lambda p: handle_by_dimension(p, "user_id"),
    "/query/by-app":         lambda p: handle_by_dimension(p, "application_id"),
    "/query/by-model":       lambda p: handle_by_dimension(p, "model_id"),
    "/query/timeseries":     handle_timeseries,
    "/query/reconciliation": handle_reconciliation,
    "/query/coverage":       handle_coverage,
    "/query/tenants":        handle_tenants,           # MSP: list tenant IDs
    "/query/tenant-summary": handle_tenant_summary,    # MSP: full client report
}


def lambda_handler(event: dict, context: Any) -> dict:
    path   = event.get("path", "")
    params = event.get("queryStringParameters") or {}

    # ── Derive tenant scope from the verified JWT (server-side authority) ──
    effective_tenant, is_admin = resolve_tenant_scope(event, params)

    # Force params.tenantId to the server-resolved value. A client cannot
    # widen their scope; an admin's optional filter is preserved.
    if effective_tenant:
        params = {**params, "tenantId": effective_tenant}
    else:
        # admin viewing all tenants → ensure no stale tenantId leaks through
        params = {k: v for k, v in params.items() if k != "tenantId"}

    logger.info(
        "Query: path=%s tenant=%s admin=%s",
        path, effective_tenant or "ALL", is_admin,
    )

    if "/query/" in path:
        normalized_path = "/query/" + path.split("/query/")[-1].lstrip("/")
    else:
        normalized_path = path

    # Admin-only endpoints
    ADMIN_ONLY = {"/query/tenants"}
    if normalized_path in ADMIN_ONLY and not is_admin:
        return _response(403, {
            "error": "Forbidden",
            "detail": "This endpoint requires an administrator account.",
        })

    handler_fn = ROUTE_MAP.get(normalized_path)
    if handler_fn is None:
        return _response(404, {
            "error":               f"Unknown endpoint: {path}",
            "normalized":          normalized_path,
            "available_endpoints": sorted(ROUTE_MAP.keys()),
        })

    try:
        result = handler_fn(params)
        return _response(200, result)
    except Exception as exc:
        tb = traceback.format_exc()
        logger.error("Query error on %s: %s\n%s", path, exc, tb)
        return _response(500, {
            "error":  "Internal query error",
            "detail": str(exc),
            "path":   normalized_path,
        })


def _response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type":                "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body, default=str),
    }