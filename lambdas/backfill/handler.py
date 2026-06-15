"""
CloudWatch Backfill Lambda — secondary coverage path.

Runs hourly. Reads Bedrock model invocation logs from CloudWatch,
compares against existing wrapper events in DynamoDB, and writes
records for any invocations that bypassed the wrapper.

Design intent (critical — do not deviate):
- This is NOT the live path. It is gap detection and coverage reporting.
- Events written here have source='cloudwatch_backfill'.
- The dashboard's live cost numbers come from wrapper events only.
- Coverage % is the metric to watch: low coverage = wrapper is being bypassed.

Prerequisites:
- Bedrock model invocation logging must be enabled in the account:
  https://docs.aws.amazon.com/bedrock/latest/userguide/model-invocation-logging.html
- The log group is typically /aws/bedrock/modelinvocations
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

import boto3

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

logs_client = boto3.client("logs")
dynamodb = boto3.resource("dynamodb")
sqs = boto3.client("sqs")

EVENTS_TABLE = os.environ["EVENTS_TABLE"]
QUEUE_URL = os.environ["COST_ENRICHMENT_QUEUE_URL"]

events_table = dynamodb.Table(EVENTS_TABLE)

# Bedrock model invocation log group (configurable per account/region)
BEDROCK_LOG_GROUP = os.getenv(
    "BEDROCK_LOG_GROUP", "/aws/bedrock/modelinvocations"
)

# How far back to look in each run. Overlap with the previous hour to catch
# events that arrived late in CloudWatch.
LOOKBACK_MINUTES = int(os.getenv("BACKFILL_LOOKBACK_MINUTES", "70"))


def get_cloudwatch_invocations(
    start_time: datetime, end_time: datetime
) -> list[dict]:
    """
    Retrieve Bedrock model invocation log entries from CloudWatch Logs Insights.

    The Bedrock invocation log schema includes:
    - modelId, inputTokenCount, outputTokenCount, requestId
    - identity (caller ARN), requestTime
    """
    query = """
        fields @timestamp, modelId, inputTokenCount, outputTokenCount,
               requestId, identity.arn
        | filter @logStream like /model-invocations/
        | sort @timestamp asc
        | limit 10000
    """

    try:
        start_query_resp = logs_client.start_query(
            logGroupName=BEDROCK_LOG_GROUP,
            startTime=int(start_time.timestamp()),
            endTime=int(end_time.timestamp()),
            queryString=query,
        )
        query_id = start_query_resp["queryId"]
    except logs_client.exceptions.ResourceNotFoundException:
        logger.warning(
            "Bedrock invocation log group %s not found. "
            "Enable model invocation logging in Bedrock console.",
            BEDROCK_LOG_GROUP,
        )
        return []

    # Poll for results
    import time

    for _ in range(30):
        time.sleep(2)
        result = logs_client.get_query_results(queryId=query_id)
        if result["status"] in ("Complete", "Failed", "Cancelled"):
            break

    if result["status"] != "Complete":
        logger.error("CloudWatch Logs query failed: %s", result["status"])
        return []

    invocations = []
    for row in result.get("results", []):
        record = {field["field"]: field["value"] for field in row}
        invocations.append(record)

    logger.info(
        "CloudWatch returned %d invocations for window %s — %s",
        len(invocations),
        start_time.isoformat(),
        end_time.isoformat(),
    )
    return invocations


def find_existing_request_ids(request_ids: list[str]) -> set[str]:
    """
    Batch-query DynamoDB to find which request_ids already have wrapper events.

    Uses a Scan with filter on the request_id field. For high volume, consider
    a separate request_id → event_id lookup table.
    """
    if not request_ids:
        return set()

    found = set()
    # Scan in chunks (DynamoDB filter is post-scan, so we scan all and filter)
    # For MVP this is acceptable; at scale add a GSI on request_id.
    paginator_kwargs = {
        "FilterExpression": "source = :src AND request_id IN ("
        + ", ".join(f":rid{i}" for i in range(len(request_ids)))
        + ")",
        "ExpressionAttributeValues": {
            ":src": "wrapper",
            **{f":rid{i}": rid for i, rid in enumerate(request_ids)},
        },
        "ProjectionExpression": "request_id",
    }

    try:
        resp = events_table.scan(**paginator_kwargs)
        for item in resp.get("Items", []):
            found.add(item.get("request_id"))
        while resp.get("LastEvaluatedKey"):
            resp = events_table.scan(
                **paginator_kwargs, ExclusiveStartKey=resp["LastEvaluatedKey"]
            )
            for item in resp.get("Items", []):
                found.add(item.get("request_id"))
    except Exception as exc:
        logger.exception("Error querying existing request IDs: %s", exc)

    return found


def write_backfill_event(cw_record: dict, account_id: str, region: str) -> Optional[str]:
    """
    Write a backfill event for a CloudWatch invocation that has no wrapper event.
    Returns the generated event_id, or None on failure.
    """
    event_id = str(uuid.uuid4())
    timestamp = cw_record.get("@timestamp", datetime.now(timezone.utc).isoformat())
    model_id = cw_record.get("modelId", "unknown")
    request_id = cw_record.get("requestId", "unknown")

    # Parse token counts — CloudWatch uses string values
    try:
        input_tokens = int(cw_record.get("inputTokenCount", 0))
        output_tokens = int(cw_record.get("outputTokenCount", 0))
    except (ValueError, TypeError):
        input_tokens = 0
        output_tokens = 0

    item = {
        "PK": f"EVENT#{event_id}",
        "SK": timestamp,
        "event_id": event_id,
        "schema_version": 1,
        "timestamp": timestamp,
        "region": region,
        "account_id": account_id,
        "model_id": model_id,
        "invocation_type": "invoke_model",  # CW logs don't distinguish; assume direct
        "user_id": "NULL",
        "agent_id": "NULL",
        "session_id": "NULL",
        "application_id": "NULL",
        "request_id": request_id,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cache_read_tokens": 0,
        "cache_write_tokens": 0,
        "latency_ms": None,
        "status": "success",
        "error_code": "NULL",
        "source": "cloudwatch_backfill",
        # No TTL on backfill events; retain indefinitely for reconciliation
    }

    try:
        events_table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(PK)",
        )
        logger.info(
            "Wrote backfill event %s for request_id=%s model=%s",
            event_id, request_id, model_id,
        )
        return event_id
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        return None  # Already exists (race condition with another backfill run)
    except Exception as exc:
        logger.exception("Failed to write backfill event: %s", exc)
        return None


def lambda_handler(event: dict, context: Any) -> dict:
    """
    Scheduled handler — runs hourly.

    1. Query CloudWatch for Bedrock invocations in the lookback window
    2. Identify invocations not covered by wrapper events (coverage gap)
    3. Write backfill events for gaps
    4. Log coverage metrics
    """
    region = os.environ.get("AWS_REGION", "us-east-1")
    account_id = boto3.client("sts").get_caller_identity()["Account"]

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=LOOKBACK_MINUTES)

    logger.info(
        "Backfill run: window %s → %s",
        start_time.isoformat(),
        end_time.isoformat(),
    )

    # Step 1: Get all CW invocations in window
    cw_invocations = get_cloudwatch_invocations(start_time, end_time)
    if not cw_invocations:
        logger.info("No CloudWatch invocations found in window")
        return {"covered": 0, "gaps": 0, "coverage_pct": 100.0}

    # Step 2: Find which ones already have wrapper events
    request_ids = [r.get("requestId", "") for r in cw_invocations if r.get("requestId")]
    existing_ids = find_existing_request_ids(request_ids)

    # Step 3: Write backfill for gaps
    gaps = [r for r in cw_invocations if r.get("requestId") not in existing_ids]
    covered = len(cw_invocations) - len(gaps)

    backfill_count = 0
    enrichment_messages = []

    for record in gaps:
        event_id = write_backfill_event(record, account_id, region)
        if event_id:
            backfill_count += 1
            enrichment_messages.append({
                "event_id": event_id,
                "pk": f"EVENT#{event_id}",
                "sk": record.get("@timestamp", ""),
                "model_id": record.get("modelId", "unknown"),
                "region": region,
                "timestamp": record.get("@timestamp", ""),
                "input_tokens": int(record.get("inputTokenCount", 0) or 0),
                "output_tokens": int(record.get("outputTokenCount", 0) or 0),
                "cache_read_tokens": 0,
                "cache_write_tokens": 0,
            })

    # Enqueue backfill events for cost enrichment (batch)
    for i in range(0, len(enrichment_messages), 10):
        batch = enrichment_messages[i:i+10]
        entries = [
            {
                "Id": str(j),
                "MessageBody": json.dumps(msg),
                "MessageGroupId": "cost-enrichment",
            }
            for j, msg in enumerate(batch)
        ]
        try:
            sqs.send_message_batch(QueueUrl=QUEUE_URL, Entries=entries)
        except Exception as exc:
            logger.error("Failed to enqueue backfill batch: %s", exc)

    # Step 4: Coverage metrics
    total = len(cw_invocations)
    coverage_pct = (covered / total * 100) if total > 0 else 100.0

    logger.info(
        "Backfill complete: total=%d covered=%d gaps=%d backfilled=%d coverage=%.1f%%",
        total, covered, len(gaps), backfill_count, coverage_pct,
    )

    if coverage_pct < 90.0:
        logger.warning(
            "LOW WRAPPER COVERAGE: %.1f%% — %d invocations bypassed the wrapper",
            coverage_pct,
            len(gaps),
        )

    return {
        "window_start": start_time.isoformat(),
        "window_end": end_time.isoformat(),
        "cw_total": total,
        "covered_by_wrapper": covered,
        "gaps_found": len(gaps),
        "backfilled": backfill_count,
        "coverage_pct": round(coverage_pct, 2),
    }
