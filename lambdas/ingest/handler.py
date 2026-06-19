"""
Ingest Lambda — hot path.

Receives a Bedrock invocation event from the SDK wrapper via API Gateway,
validates it, writes a raw record to DynamoDB, and enqueues it for async
cost enrichment.

Design intent:
- This is the HOT PATH. Keep it fast and simple.
- Do NOT look up prices here. That belongs in cost_compute.
- Do NOT call any external services beyond DynamoDB + SQS.
- Fail loudly on schema violations so wrapper bugs surface immediately.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import boto3
from boto3.dynamodb.types import TypeSerializer

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

dynamodb = boto3.resource("dynamodb")
sqs = boto3.client("sqs")

EVENTS_TABLE = os.environ["EVENTS_TABLE"]
QUEUE_URL = os.environ["COST_ENRICHMENT_QUEUE_URL"]
EVENT_RETENTION_DAYS = int(os.environ.get("EVENT_RETENTION_DAYS", "90"))

table = dynamodb.Table(EVENTS_TABLE)

# Fields required on every incoming event
REQUIRED_FIELDS = {
    "event_id",
    "schema_version",
    "timestamp",
    "region",
    "account_id",
    "model_id",
    "invocation_type",
    "input_tokens",
    "output_tokens",
    "status",
    "source",
}

VALID_INVOCATION_TYPES = {"invoke_model", "converse", "agent_step"}
VALID_STATUSES         = {"success", "error", "throttled"}

# Internal sentinel values used by the platform itself.
# Any other non-empty string is treated as a client/tenant ID.
INTERNAL_SOURCES = {"wrapper", "cloudwatch_backfill"}


def _is_valid_source(source: str) -> bool:
    """
    Accept internal sentinels OR any non-empty alphanumeric-with-hyphens
    string as a tenant/client ID (e.g. 'client-alpha', 'acme-corp').
    """
    if not source or not isinstance(source, str):
        return False
    if source in INTERNAL_SOURCES:
        return True
    # Tenant IDs: 1-64 chars, letters/digits/hyphens/underscores only
    import re
    return bool(re.match(r'^[a-zA-Z0-9_-]{1,64}$', source))


def validate_event(event: dict) -> list[str]:
    """Return a list of validation errors. Empty list = valid."""
    errors = []

    missing = REQUIRED_FIELDS - set(event.keys())
    if missing:
        errors.append(f"Missing required fields: {sorted(missing)}")
        return errors  # stop early; further checks would fail

    if event["invocation_type"] not in VALID_INVOCATION_TYPES:
        errors.append(
            f"Invalid invocation_type: {event['invocation_type']!r}. "
            f"Must be one of {VALID_INVOCATION_TYPES}"
        )

    if event["status"] not in VALID_STATUSES:
        errors.append(f"Invalid status: {event['status']!r}")

    if not _is_valid_source(event["source"]):
        errors.append(
            f"Invalid source: {event['source']!r}. "
            f"Must be 'wrapper', 'cloudwatch_backfill', or a valid tenant ID "
            f"(letters, digits, hyphens, underscores, 1-64 chars)."
        )

    try:
        datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        errors.append(f"Invalid timestamp format: {event['timestamp']!r}")

    for field in ("input_tokens", "output_tokens"):
        if not isinstance(event[field], int) or event[field] < 0:
            errors.append(f"{field} must be a non-negative integer")

    return errors


def build_dynamodb_item(event: dict) -> dict:
    """
    Convert the raw event dict into the DynamoDB item format.

    PK = EVENT#<event_id>
    SK = <timestamp_iso>  (ISO 8601, lexicographically sortable)

    All GSI partition keys are written even if null, using the sentinel
    string 'NULL' — DynamoDB GSIs can't index missing attributes, and
    we need all events to be queriable across all dimensions.
    """
    ttl = int(time.time()) + (EVENT_RETENTION_DAYS * 86400)

    def nullable(val: Any) -> str:
        """Store None as 'NULL' sentinel so GSIs can index it."""
        return val if val is not None else "NULL"

    item = {
        "PK": f"EVENT#{event['event_id']}",
        "SK": event["timestamp"],
        # Identity
        "event_id": event["event_id"],
        "schema_version": event["schema_version"],
        # Time / location
        "timestamp": event["timestamp"],
        "region": event["region"],
        "account_id": event["account_id"],
        # Invocation
        "model_id": event["model_id"],
        "invocation_type": event["invocation_type"],
        # Attribution (nullable → sentinel for GSI indexability)
        "user_id": nullable(event.get("user_id")),
        "agent_id": nullable(event.get("agent_id")),
        "session_id": nullable(event.get("session_id")),
        "application_id": nullable(event.get("application_id")),
        "request_id": nullable(event.get("request_id")),
        # Tokens
        "input_tokens": event["input_tokens"],
        "output_tokens": event["output_tokens"],
        "cache_read_tokens": event.get("cache_read_tokens", 0),
        "cache_write_tokens": event.get("cache_write_tokens", 0),
        # Cost — NOT computed here; enriched async by cost_compute Lambda
        # Omit entirely so conditional updates in cost_compute are clean
        # "computed_cost_usd": None,
        # Operational
        "latency_ms": event.get("latency_ms"),
        "status": event["status"],
        "error_code": nullable(event.get("error_code")),
        "source": event["source"],
        # TTL
        "ttl": ttl,
    }

    return item


def lambda_handler(event: dict, context: Any) -> dict:
    """
    API Gateway proxy integration handler.

    Expected: POST /events with JSON body matching the invocation event schema.
    Returns: 200 on success, 400 on validation error, 500 on internal error.
    """
    try:
        body_raw = event.get("body", "{}")
        body = json.loads(body_raw) if isinstance(body_raw, str) else body_raw
    except json.JSONDecodeError as exc:
        logger.warning("Failed to parse request body: %s", exc)
        return _response(400, {"error": "Invalid JSON body"})

    # Validate
    errors = validate_event(body)
    if errors:
        logger.warning("Event validation failed: %s", errors)
        return _response(400, {"error": "Validation failed", "details": errors})

    # Write to DynamoDB
    item = build_dynamodb_item(body)
    try:
        table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(PK)",  # idempotency guard
        )
        logger.info(
            "Stored event %s model=%s tokens_in=%d tokens_out=%d",
            body["event_id"],
            body["model_id"],
            body["input_tokens"],
            body["output_tokens"],
        )
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        # Duplicate event_id — idempotent; return 200
        logger.info("Duplicate event_id %s — skipping write", body["event_id"])
        return _response(200, {"status": "duplicate", "event_id": body["event_id"]})
    except Exception as exc:
        logger.exception("DynamoDB write failed: %s", exc)
        return _response(500, {"error": "Storage failure"})

    # Enqueue for async cost enrichment
    try:
        sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(
                {
                    "event_id": body["event_id"],
                    "pk": item["PK"],
                    "sk": item["SK"],
                    "model_id": body["model_id"],
                    "region": body["region"],
                    "timestamp": body["timestamp"],
                    "input_tokens": body["input_tokens"],
                    "output_tokens": body["output_tokens"],
                    "cache_read_tokens": body.get("cache_read_tokens", 0),
                    "cache_write_tokens": body.get("cache_write_tokens", 0),
                }
            ),
            MessageGroupId="cost-enrichment",  # for potential FIFO migration
        )
    except Exception as exc:
        # Non-fatal: DynamoDB write succeeded; cost compute can retry via scheduled backfill
        logger.error(
            "Failed to enqueue event %s for cost enrichment: %s",
            body["event_id"],
            exc,
        )

    return _response(200, {"status": "accepted", "event_id": body["event_id"]})


def _response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "X-Content-Type-Options": "nosniff",
        },
        "body": json.dumps(body),
    }
