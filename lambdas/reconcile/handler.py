"""
CUR Reconciliation Lambda — daily drift detection.

Runs at 06:00 UTC daily. Reads yesterday's Cost and Usage Report from S3,
sums up what AWS actually billed for Bedrock, compares against our computed
cost sum from DynamoDB, and writes a variance report.

Design intent (critical — do not deviate):
- CUR is GROUND TRUTH for reconciliation, NOT for live cost numbers.
- The variance report surfaces: price drift, discounts, Savings Plans,
  and Reserved Throughput pricing that our price table doesn't know about.
- A large positive variance (billed > computed) may indicate: discount not
  reflected in price table, or a model price change. Investigate + update
  price table.
- A large negative variance (computed > billed) may indicate: phantom events,
  price table too high, or Reserved Throughput amortization.
"""

import csv
import gzip
import io
import json
import logging
import os
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Optional

import boto3
from boto3.dynamodb.conditions import Key, Attr

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

dynamodb = boto3.resource("dynamodb")
s3_client = boto3.client("s3")
sns_client = boto3.client("sns")

EVENTS_TABLE = os.environ["EVENTS_TABLE"]
RECONCILIATION_TABLE = os.environ["RECONCILIATION_TABLE"]
ALERT_TOPIC_ARN = os.environ["ALERT_TOPIC_ARN"]
CUR_BUCKET = os.environ.get("CUR_BUCKET")

events_table = dynamodb.Table(EVENTS_TABLE)
recon_table = dynamodb.Table(RECONCILIATION_TABLE)

# Alert if variance exceeds this threshold (absolute %)
VARIANCE_ALERT_THRESHOLD_PCT = float(
    os.getenv("VARIANCE_ALERT_THRESHOLD_PCT", "10.0")
)


def get_computed_cost_for_date(date_str: str) -> dict:
    """
    Sum computed costs from DynamoDB for all events on the given UTC date.

    Returns: {
        'total_cost_usd': Decimal,
        'event_count': int,
        'by_model': {model_id: Decimal},
        'unenriched_count': int,  # events without computed_cost_usd
    }
    """
    # Date window
    start_ts = f"{date_str}T00:00:00.000Z"
    end_ts = f"{date_str}T23:59:59.999Z"

    total_cost = Decimal("0")
    by_model: dict[str, Decimal] = {}
    event_count = 0
    unenriched_count = 0

    # Scan events table for the date window
    # Note: For high scale, replace with a date-partitioned GSI or Athena query
    scan_kwargs = {
        "FilterExpression": (
            Attr("timestamp").between(start_ts, end_ts)
            & Attr("source").eq("wrapper")
        ),
        "ProjectionExpression": "event_id, model_id, computed_cost_usd, timestamp",
    }

    resp = events_table.scan(**scan_kwargs)
    items = resp.get("Items", [])

    while resp.get("LastEvaluatedKey"):
        resp = events_table.scan(
            **scan_kwargs, ExclusiveStartKey=resp["LastEvaluatedKey"]
        )
        items.extend(resp.get("Items", []))

    for item in items:
        event_count += 1
        model_id = item.get("model_id", "unknown")
        cost = item.get("computed_cost_usd")
        if cost is not None:
            cost_dec = Decimal(str(cost))
            total_cost += cost_dec
            by_model[model_id] = by_model.get(model_id, Decimal("0")) + cost_dec
        else:
            unenriched_count += 1

    logger.info(
        "Computed cost for %s: $%s across %d events (%d unenriched)",
        date_str, total_cost, event_count, unenriched_count,
    )

    return {
        "total_cost_usd": total_cost,
        "event_count": event_count,
        "by_model": by_model,
        "unenriched_count": unenriched_count,
    }


def get_cur_cost_for_date(date_str: str) -> Optional[dict]:
    """
    Read the Cost and Usage Report from S3 and extract Bedrock charges for the date.

    CUR is a CSV (gzipped) with one row per line item. Bedrock line items have:
    - lineItem/ProductCode = 'AmazonBedrock'
    - lineItem/UsageStartDate = the billing period
    - lineItem/UnblendedCost = actual charge

    Returns: {
        'total_cost_usd': Decimal,
        'by_model': {model_id: Decimal},
    }
    or None if CUR is not configured or not yet available.
    """
    if not CUR_BUCKET:
        logger.info("CUR_BUCKET not configured — skipping CUR ingestion")
        return None

    # CUR files are typically organized as:
    # s3://<bucket>/<prefix>/YYYY/MM/DD/<report_name>-<hash>.csv.gz
    # Adjust the prefix to match your CUR configuration
    year, month, day = date_str.split("-")
    prefix = os.getenv("CUR_PREFIX", f"cur/bedrock-cost-explorer/{year}/{month}/")

    try:
        resp = s3_client.list_objects_v2(Bucket=CUR_BUCKET, Prefix=prefix)
    except Exception as exc:
        logger.error("Failed to list CUR objects from s3://%s/%s: %s", CUR_BUCKET, prefix, exc)
        return None

    objects = resp.get("Contents", [])
    if not objects:
        logger.warning(
            "No CUR files found at s3://%s/%s — CUR may not be available yet",
            CUR_BUCKET, prefix,
        )
        return None

    total_cost = Decimal("0")
    by_model: dict[str, Decimal] = {}

    for obj in objects:
        key = obj["Key"]
        if not (key.endswith(".csv.gz") or key.endswith(".csv")):
            continue

        try:
            s3_resp = s3_client.get_object(Bucket=CUR_BUCKET, Key=key)
            body = s3_resp["Body"].read()

            if key.endswith(".gz"):
                body = gzip.decompress(body)

            reader = csv.DictReader(io.StringIO(body.decode("utf-8")))
            for row in reader:
                # Filter to Bedrock charges only
                if row.get("lineItem/ProductCode") != "AmazonBedrock":
                    continue

                # Filter to the target date
                usage_start = row.get("lineItem/UsageStartDate", "")
                if not usage_start.startswith(date_str):
                    continue

                cost_str = row.get("lineItem/UnblendedCost", "0")
                try:
                    cost = Decimal(cost_str)
                except Exception:
                    continue

                total_cost += cost

                # Extract model from usage description (e.g. "Claude 3.5 Sonnet input tokens")
                description = row.get("lineItem/UsageType", "")
                model_key = description.split(":")[0] if ":" in description else description
                by_model[model_key] = by_model.get(model_key, Decimal("0")) + cost

        except Exception as exc:
            logger.error("Failed to process CUR file %s: %s", key, exc)
            continue

    logger.info("CUR cost for %s: $%s", date_str, total_cost)
    return {"total_cost_usd": total_cost, "by_model": by_model}


def write_reconciliation_record(date_str: str, result: dict):
    """Write the reconciliation summary to DynamoDB."""
    recon_table.put_item(Item={
        "PK": f"RUN#{date_str}",
        "SK": "SUMMARY",
        "run_date": date_str,
        "computed_cost_usd": result["computed_cost_usd"],
        "billed_cost_usd": result["billed_cost_usd"],
        "variance_usd": result["variance_usd"],
        "variance_pct": result["variance_pct"],
        "event_count": result["event_count"],
        "unenriched_count": result["unenriched_count"],
        "cur_available": result["cur_available"],
        "notes": result.get("notes", ""),
        "created_at": datetime.now(timezone.utc).isoformat(),
    })

    # Write per-model breakdown
    for model_id, computed in result["by_model_computed"].items():
        recon_table.put_item(Item={
            "PK": f"RUN#{date_str}",
            "SK": f"MODEL#{model_id}",
            "run_date": date_str,
            "model_id": model_id,
            "computed_cost_usd": computed,
            "created_at": datetime.now(timezone.utc).isoformat(),
        })


def publish_variance_alert(date_str: str, result: dict):
    """Publish an SNS alert if variance exceeds threshold."""
    variance_pct = abs(float(result["variance_pct"]))
    if variance_pct < VARIANCE_ALERT_THRESHOLD_PCT:
        return

    direction = "OVERBILLED" if result["variance_usd"] > 0 else "UNDERBILLED"
    message = f"""
Bedrock Cost Explorer — Reconciliation Alert
Date: {date_str}
Status: {direction}

Computed cost: ${result['computed_cost_usd']:.6f}
AWS billed:    ${result['billed_cost_usd']:.6f}
Variance:      ${result['variance_usd']:.6f} ({result['variance_pct']:.2f}%)

Event count: {result['event_count']}
Unenriched events: {result['unenriched_count']}

Possible causes:
- {'Positive variance (AWS > computed): price change, discount, Savings Plan, or Reserved Throughput' if result['variance_usd'] > 0 else 'Negative variance (computed > AWS): price table too high, phantom events, or Reserved Throughput amortization'}
- Check price table version and update if AWS changed prices

Action: Review the reconciliation record at PK=RUN#{date_str} in bedrock_reconciliation_runs table.
"""
    sns_client.publish(
        TopicArn=ALERT_TOPIC_ARN,
        Subject=f"[Bedrock Cost] Reconciliation variance {result['variance_pct']:.1f}% on {date_str}",
        Message=message,
    )
    logger.warning("Variance alert published for %s: %.2f%%", date_str, variance_pct)


def lambda_handler(event: dict, context: Any) -> dict:
    """
    Daily scheduled handler — runs at 06:00 UTC.

    Reconciles yesterday's computed costs against the CUR.
    """
    # Default to yesterday; allow override via event payload for manual runs
    if "date" in event:
        date_str = event["date"]  # YYYY-MM-DD
    else:
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        date_str = yesterday.strftime("%Y-%m-%d")

    logger.info("Reconciling costs for %s", date_str)

    # Get computed costs from DynamoDB
    computed = get_computed_cost_for_date(date_str)

    # Get billed costs from CUR
    cur = get_cur_cost_for_date(date_str)
    cur_available = cur is not None

    billed_cost = cur["total_cost_usd"] if cur else Decimal("0")
    variance = billed_cost - computed["total_cost_usd"]
    variance_pct = (
        (variance / billed_cost * 100)
        if billed_cost != 0
        else Decimal("0")
    )

    result = {
        "date": date_str,
        "computed_cost_usd": computed["total_cost_usd"],
        "billed_cost_usd": billed_cost,
        "variance_usd": variance,
        "variance_pct": variance_pct.quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        ) if isinstance(variance_pct, Decimal) else Decimal(str(round(variance_pct, 2))),
        "event_count": computed["event_count"],
        "unenriched_count": computed["unenriched_count"],
        "cur_available": cur_available,
        "by_model_computed": computed["by_model"],
        "notes": "" if cur_available else "CUR not available; billed_cost_usd is 0",
    }

    write_reconciliation_record(date_str, result)
    publish_variance_alert(date_str, result)

    logger.info(
        "Reconciliation complete for %s: computed=$%s billed=$%s variance=$%s (%.2f%%)",
        date_str,
        result["computed_cost_usd"],
        result["billed_cost_usd"],
        result["variance_usd"],
        result["variance_pct"],
    )

    return {
        "date": date_str,
        "computed_cost_usd": str(result["computed_cost_usd"]),
        "billed_cost_usd": str(result["billed_cost_usd"]),
        "variance_usd": str(result["variance_usd"]),
        "variance_pct": str(result["variance_pct"]),
        "cur_available": cur_available,
    }
