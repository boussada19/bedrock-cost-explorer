"""
Price table seed script.

Run this once after deploying the CDK stack to populate the DynamoDB
price table with current AWS Bedrock pricing.

Also run whenever AWS changes Bedrock prices — create a new version
rather than updating existing rows. Historical events keep the version
that was active when they fired.

Usage:
    python seed_prices.py [--table bedrock_price_table] [--region us-east-1]

Prices below are as of mid-2025. Verify against:
https://aws.amazon.com/bedrock/pricing/
"""

import argparse
import json
from datetime import datetime, timezone
from decimal import Decimal

import boto3

# ──────────────────────────────────────────────────────────────────────────────
# PRICE DATA
# Format: (model_id, region, price_type, price_per_1k_tokens_usd)
#
# region '*' = wildcard fallback for regions not listed explicitly.
# Bedrock pricing varies by region; add per-region rows as needed.
#
# IMPORTANT: prices change. This file is authoritative for what's in the
# price table. The reconciliation job will flag drift against CUR.
# ──────────────────────────────────────────────────────────────────────────────

PRICES_V1 = {
    "version_id": 1,
    "effective_from": "2025-01-01T00:00:00Z",
    "effective_until": "9999-12-31T23:59:59Z",  # sentinel for "currently active"
    "notes": "Initial seed — Bedrock on-demand pricing, mid-2025",
    "entries": [
        # ── Anthropic Claude (on-demand, us-east-1 / us-west-2) ──
        # Claude 3.5 Haiku
        ("anthropic.claude-haiku-3-5", "*", "input",  "0.00080"),
        ("anthropic.claude-haiku-3-5", "*", "output", "0.00400"),
        ("anthropic.claude-haiku-3-5", "*", "cache_read",  "0.00008"),
        ("anthropic.claude-haiku-3-5", "*", "cache_write", "0.00100"),
        # Claude 3.5 Sonnet
        ("anthropic.claude-sonnet-3-5", "*", "input",  "0.00300"),
        ("anthropic.claude-sonnet-3-5", "*", "output", "0.01500"),
        ("anthropic.claude-sonnet-3-5", "*", "cache_read",  "0.00030"),
        ("anthropic.claude-sonnet-3-5", "*", "cache_write", "0.00375"),
        # Claude 3 Opus
        ("anthropic.claude-opus-3",    "*", "input",  "0.01500"),
        ("anthropic.claude-opus-3",    "*", "output", "0.07500"),
        ("anthropic.claude-opus-3",    "*", "cache_read",  "0.00150"),
        ("anthropic.claude-opus-3",    "*", "cache_write", "0.01875"),
        # Claude 3 Haiku
        ("anthropic.claude-haiku-3",   "*", "input",  "0.00025"),
        ("anthropic.claude-haiku-3",   "*", "output", "0.00125"),
        # Claude Instant (legacy)
        ("anthropic.claude-instant-v1", "*", "input",  "0.00080"),
        ("anthropic.claude-instant-v1", "*", "output", "0.00240"),
        # Claude 2.x (legacy)
        ("anthropic.claude-v2",        "*", "input",  "0.00800"),
        ("anthropic.claude-v2",        "*", "output", "0.02400"),
        ("anthropic.claude-v2:1",      "*", "input",  "0.00800"),
        ("anthropic.claude-v2:1",      "*", "output", "0.02400"),

        # ── Amazon Titan ──
        ("amazon.titan-text-lite-v1",    "*", "input",  "0.00030"),
        ("amazon.titan-text-lite-v1",    "*", "output", "0.00040"),
        ("amazon.titan-text-express-v1", "*", "input",  "0.00200"),
        ("amazon.titan-text-express-v1", "*", "output", "0.00060"),
        ("amazon.titan-text-premier-v1", "*", "input",  "0.00050"),
        ("amazon.titan-text-premier-v1", "*", "output", "0.00150"),

        # ── Meta Llama ──
        ("meta.llama3-8b-instruct-v1",  "*", "input",  "0.00022"),
        ("meta.llama3-8b-instruct-v1",  "*", "output", "0.00022"),
        ("meta.llama3-70b-instruct-v1", "*", "input",  "0.00099"),
        ("meta.llama3-70b-instruct-v1", "*", "output", "0.00099"),
        ("meta.llama3-1-405b-instruct-v1", "*", "input",  "0.00532"),
        ("meta.llama3-1-405b-instruct-v1", "*", "output", "0.01600"),

        # ── Mistral ──
        ("mistral.mistral-7b-instruct-v0:2",   "*", "input",  "0.00015"),
        ("mistral.mistral-7b-instruct-v0:2",   "*", "output", "0.00020"),
        ("mistral.mixtral-8x7b-instruct-v0:1", "*", "input",  "0.00045"),
        ("mistral.mixtral-8x7b-instruct-v0:1", "*", "output", "0.00070"),
        ("mistral.mistral-large-2402-v1:0",    "*", "input",  "0.00400"),
        ("mistral.mistral-large-2402-v1:0",    "*", "output", "0.01200"),

        # ── AI21 Jamba ──
        ("ai21.jamba-1-5-large-v1:0", "*", "input",  "0.00200"),
        ("ai21.jamba-1-5-large-v1:0", "*", "output", "0.00800"),
        ("ai21.jamba-1-5-mini-v1:0",  "*", "input",  "0.00020"),
        ("ai21.jamba-1-5-mini-v1:0",  "*", "output", "0.00040"),

        # ── Cohere ──
        ("cohere.command-r-v1:0",      "*", "input",  "0.00050"),
        ("cohere.command-r-v1:0",      "*", "output", "0.00150"),
        ("cohere.command-r-plus-v1:0", "*", "input",  "0.00300"),
        ("cohere.command-r-plus-v1:0", "*", "output", "0.01500"),
    ],
}


def seed_price_table(table_name: str, region: str):
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(table_name)

    version = PRICES_V1
    version_id = version["version_id"]

    print(f"\nSeeding price table: {table_name}")
    print(f"Version: {version_id} | Effective: {version['effective_from']}")
    print(f"Entries: {len(version['entries'])}")
    print("-" * 60)

    with table.batch_writer() as batch:
        for model_id, region_code, price_type, price_str in version["entries"]:
            sk = f"{model_id}#{region_code}#{price_type}"
            item = {
                "PK": f"VERSION#{version_id}",
                "SK": sk,
                "version_id": version_id,
                "effective_from": version["effective_from"],
                "effective_until": version["effective_until"],
                "model_id": model_id,
                "region": region_code,
                "price_type": price_type,
                "price_per_1k_tokens": Decimal(price_str),
                "notes": version["notes"],
            }
            batch.put_item(Item=item)
            print(f"  + {model_id:<50} {region_code:<12} {price_type:<12} ${price_str}/1k")

    print(f"\n✓ Seeded {len(version['entries'])} price entries (version {version_id})")


def add_new_version(
    table_name: str,
    region: str,
    version_id: int,
    effective_from: str,
    new_entries: list,
    notes: str,
):
    """
    Add a new price version without modifying existing versions.
    The old version's effective_until should be set to the new version's
    effective_from (exclusive). This must be done as a separate update.

    Call this when AWS changes prices.
    """
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(table_name)

    print(f"\nAdding price version {version_id}: {notes}")
    print(f"Effective from: {effective_from}")

    with table.batch_writer() as batch:
        for model_id, region_code, price_type, price_str in new_entries:
            sk = f"{model_id}#{region_code}#{price_type}"
            item = {
                "PK": f"VERSION#{version_id}",
                "SK": sk,
                "version_id": version_id,
                "effective_from": effective_from,
                "effective_until": "9999-12-31T23:59:59Z",
                "model_id": model_id,
                "region": region_code,
                "price_type": price_type,
                "price_per_1k_tokens": Decimal(price_str),
                "notes": notes,
            }
            batch.put_item(Item=item)

    # Close out the previous version
    # (In production, query all previous VERSION#<prev> entries and update effective_until)
    print(f"✓ Version {version_id} written. Remember to set effective_until on version {version_id - 1}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed Bedrock price table")
    parser.add_argument(
        "--table", default="bedrock_price_table", help="DynamoDB table name"
    )
    parser.add_argument(
        "--region", default="us-east-1", help="AWS region"
    )
    args = parser.parse_args()

    seed_price_table(args.table, args.region)
