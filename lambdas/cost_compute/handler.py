"""
Cost Compute Lambda — async enrichment.

Consumes events from the SQS queue, looks up the versioned price for
the model at the time of the invocation, computes the USD cost, and
updates the DynamoDB event record.

Design intent:
- This is NOT in the critical path of the Bedrock call. Run async.
- Price lookup uses the event's timestamp to find the correct version,
  so historical events always use the price that was active then.
- Computed cost is: (input_tokens / 1000 * input_price)
                   + (output_tokens / 1000 * output_price)
                   + cache terms if non-zero
"""

import json
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Optional

import boto3
from boto3.dynamodb.conditions import Key, Attr

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

dynamodb = boto3.resource("dynamodb")
EVENTS_TABLE = os.environ["EVENTS_TABLE"]
PRICE_TABLE = os.environ["PRICE_TABLE"]

events_table = dynamodb.Table(EVENTS_TABLE)
price_table = dynamodb.Table(PRICE_TABLE)

# In-Lambda price cache to avoid re-fetching across SQS batch items
# Keyed by (model_id, region, version_id) — valid within a Lambda execution
_price_cache: dict[str, dict] = {}


def get_price_for_event(
    model_id: str, region: str, event_timestamp: str
) -> Optional[dict]:
    """
    Find the price table entry valid at event_timestamp for the given model+region.

    Lookup strategy:
    1. Query gsi_active_prices for currently-active prices (effective_until = sentinel)
    2. If event_timestamp falls within active range, use it
    3. Otherwise, scan for historical versions (rare; only on late enrichment of old events)
    4. Fall back to region='*' if no region-specific price found

    Returns dict with keys: input, output, cache_read, cache_write (all Decimal per 1k tokens)
    """
    event_dt = datetime.fromisoformat(event_timestamp.replace("Z", "+00:00"))

    # Try to find prices in cache or by querying active prices first
    for target_region in (region, "*"):
        cache_key = f"{model_id}#{target_region}"

        if cache_key not in _price_cache:
            # Query the active prices GSI
            resp = price_table.query(
                IndexName="gsi_active_prices",
                KeyConditionExpression=(
                    Key("effective_until").eq("9999-12-31T23:59:59Z")
                    & Key("model_id").begins_with(model_id)
                ),
                FilterExpression=Attr("region").eq(target_region),
            )
            _price_cache[cache_key] = resp.get("Items", [])

        active_items = _price_cache[cache_key]

        if active_items:
            # Check if the event timestamp falls within this version's range
            for item in active_items:
                effective_from = datetime.fromisoformat(
                    item["effective_from"].replace("Z", "+00:00")
                )
                if event_dt >= effective_from:
                    # Build price dict from this version
                    prices = _items_to_price_dict(active_items)
                    if prices:
                        return prices

        # Historical lookup: scan for version ranges that cover event_timestamp
        # This only happens for late enrichment of events older than the current version
        hist_resp = price_table.scan(
            FilterExpression=(
                Attr("model_id").eq(model_id)
                & Attr("region").eq(target_region)
                & Attr("effective_from").lte(event_timestamp)
                & Attr("effective_until").gte(event_timestamp)
            )
        )
        hist_items = hist_resp.get("Items", [])
        if hist_items:
            return _items_to_price_dict(hist_items)

    logger.warning(
        "No price found for model=%s region=%s timestamp=%s",
        model_id,
        region,
        event_timestamp,
    )
    return None


def _items_to_price_dict(items: list[dict]) -> dict:
    """Convert a list of price table rows into a {price_type: Decimal} dict."""
    result = {}
    for item in items:
        result[item["price_type"]] = Decimal(str(item["price_per_1k_tokens"]))
    return result if result else {}


def compute_cost(
    input_tokens: int,
    output_tokens: int,
    cache_read_tokens: int,
    cache_write_tokens: int,
    prices: dict,
) -> Decimal:
    """
    Compute total USD cost from token counts and per-1k-token prices.

    Formula:
        cost = (input_tokens / 1000 * prices['input'])
               + (output_tokens / 1000 * prices['output'])
               + (cache_read_tokens / 1000 * prices.get('cache_read', 0))
               + (cache_write_tokens / 1000 * prices.get('cache_write', 0))
    """
    THOUSAND = Decimal("1000")
    cost = (
        (Decimal(input_tokens) / THOUSAND * prices.get("input", Decimal("0")))
        + (Decimal(output_tokens) / THOUSAND * prices.get("output", Decimal("0")))
        + (
            Decimal(cache_read_tokens)
            / THOUSAND
            * prices.get("cache_read", Decimal("0"))
        )
        + (
            Decimal(cache_write_tokens)
            / THOUSAND
            * prices.get("cache_write", Decimal("0"))
        )
    )
    # Round to 8 decimal places (sub-cent precision for high-volume analytics)
    return cost.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)


def enrich_event(message_body: dict) -> bool:
    """
    Enrich a single event with computed cost. Returns True on success.
    """
    event_id = message_body["event_id"]
    pk = message_body["pk"]
    sk = message_body["sk"]
    model_id = message_body["model_id"]
    region = message_body["region"]
    timestamp = message_body["timestamp"]

    prices = get_price_for_event(model_id, region, timestamp)
    if prices is None:
        logger.error(
            "Cannot enrich event %s — no price found for model=%s region=%s",
            event_id,
            model_id,
            region,
        )
        # Return True to remove from queue — will be flagged in reconciliation
        return True

    cost = compute_cost(
        input_tokens=message_body["input_tokens"],
        output_tokens=message_body["output_tokens"],
        cache_read_tokens=message_body.get("cache_read_tokens", 0),
        cache_write_tokens=message_body.get("cache_write_tokens", 0),
        prices=prices,
    )

    # Determine price table version from the prices we found
    # (version_id is included in price table items)
    price_version = prices.get("version_id", 0)

    try:
        events_table.update_item(
            Key={"PK": pk, "SK": sk},
            UpdateExpression=(
                "SET computed_cost_usd = :cost, "
                "price_table_version = :version"
            ),
            ConditionExpression="attribute_exists(PK)",
            ExpressionAttributeValues={
                ":cost": cost,
                ":version": Decimal(str(price_version)),
            },
        )
        logger.info(
            "Enriched event %s: model=%s cost=$%s",
            event_id,
            model_id,
            cost,
        )
        return True
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        logger.warning(
            "Event %s not found in DynamoDB — may have been deleted (TTL?)",
            event_id,
        )
        return True  # Don't retry a deleted record
    except Exception as exc:
        logger.exception("Failed to update event %s: %s", event_id, exc)
        return False


def lambda_handler(event: dict, context: Any) -> dict:
    """
    SQS trigger handler. Processes a batch of enrichment messages.

    Returns partial batch failure response so SQS only retries
    messages that failed, not the entire batch.
    """
    failed_message_ids = []

    for record in event.get("Records", []):
        message_id = record["messageId"]
        try:
            body = json.loads(record["body"])
            success = enrich_event(body)
            if not success:
                failed_message_ids.append(message_id)
        except Exception as exc:
            logger.exception(
                "Unexpected error processing message %s: %s", message_id, exc
            )
            failed_message_ids.append(message_id)

    if failed_message_ids:
        logger.warning(
            "%d/%d messages failed enrichment — will retry",
            len(failed_message_ids),
            len(event["Records"]),
        )

    return {
        "batchItemFailures": [
            {"itemIdentifier": mid} for mid in failed_message_ids
        ]
    }
