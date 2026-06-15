"""
conftest.py — pytest session-level setup.

Sets the environment variables that Lambda handlers read at module import time
(e.g. os.environ["EVENTS_TABLE"]).  Must be loaded before any handler module
is imported, which pytest guarantees for conftest.py files.

Works on all platforms (Windows, Linux, macOS) without needing to pre-set
variables in the shell before running pytest.
"""

import os
import pytest


def pytest_configure(config):
    """
    Called immediately when pytest starts, before any test collection.
    This is the correct hook for env vars that are consumed at module
    import time — monkeypatch is too late because it runs per-test.
    """
    env_defaults = {
        # Lambda env vars
        "EVENTS_TABLE": "bedrock_events",
        "PRICE_TABLE": "bedrock_price_table",
        "RECONCILIATION_TABLE": "bedrock_reconciliation_runs",
        "COST_ENRICHMENT_QUEUE_URL": (
            "https://sqs.eu-central-1.amazonaws.com/123456789012/bedrock-cost-enrichment"
        ),
        "ALERT_TOPIC_ARN": (
            "arn:aws:sns:eu-central-1:123456789012:bedrock-cost-alerts"
        ),
        "EVENT_RETENTION_DAYS": "90",
        "LOG_LEVEL": "WARNING",  # suppress INFO noise in test output
        # Fake AWS credentials so boto3 doesn't hit real AWS or error on missing creds
        "AWS_DEFAULT_REGION": "eu-central-1",
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_SECURITY_TOKEN": "testing",
        "AWS_SESSION_TOKEN": "testing",
    }

    for key, value in env_defaults.items():
        # Only set if not already set — allows CI to override via real secrets
        if not os.environ.get(key):
            os.environ[key] = value
