"""
Tests for core Bedrock Cost Explorer components.

Run with: pytest tests/ -v

Uses importlib.util.spec_from_file_location for all handler imports so that
module loading works correctly on Windows, Linux, and macOS regardless of
how pytest is invoked or what the working directory is.

Environment variables are set in conftest.py before any module is imported.
"""

import importlib.util
import json
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import boto3
import pytest

# ── Path helpers ──────────────────────────────────────────────────────────────

# Root of the project (one level up from tests/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def load_module(relative_path: str, module_name: str):
    """
    Load a Python module from a path relative to the project root.
    Works on Windows and Unix without relying on sys.path ordering.
    """
    full_path = PROJECT_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(module_name, full_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ── Module-level imports (env vars set in conftest.py before this runs) ───────

ingest_handler = load_module("lambdas/ingest/handler.py", "ingest_handler")
cost_compute_handler = load_module("lambdas/cost_compute/handler.py", "cost_compute_handler")
bedrock_wrapper = load_module("sdk/python/bedrock_cost_wrapper.py", "bedrock_wrapper")


# ── Fixtures ──────────────────────────────────────────────────────────────────

def make_valid_event(**overrides) -> dict:
    """Return a valid invocation event dict."""
    base = {
        "event_id": str(uuid.uuid4()),
        "schema_version": 1,
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "region": "eu-central-1",
        "account_id": "123456789012",
        "model_id": "anthropic.claude-sonnet-4-5",
        "invocation_type": "converse",
        "user_id": "user_abc",
        "agent_id": None,
        "session_id": "sess_xyz",
        "application_id": "my-app",
        "request_id": "amzn-abc123",
        "input_tokens": 512,
        "output_tokens": 128,
        "cache_read_tokens": 0,
        "cache_write_tokens": 0,
        "latency_ms": 800,
        "status": "success",
        "error_code": None,
        "source": "wrapper",
    }
    base.update(overrides)
    return base


# ── Ingest validation tests ───────────────────────────────────────────────────

class TestIngestValidation:
    """Tests for the ingest Lambda's validate_event function."""

    def test_valid_event_passes(self):
        errors = ingest_handler.validate_event(make_valid_event())
        assert errors == []

    def test_missing_required_field_fails(self):
        event = make_valid_event()
        del event["input_tokens"]
        errors = ingest_handler.validate_event(event)
        assert any("input_tokens" in e for e in errors)

    def test_invalid_invocation_type_fails(self):
        errors = ingest_handler.validate_event(
            make_valid_event(invocation_type="not_a_type")
        )
        assert len(errors) == 1
        assert "invocation_type" in errors[0]

    def test_invalid_status_fails(self):
        errors = ingest_handler.validate_event(make_valid_event(status="pending"))
        assert any("status" in e for e in errors)

    def test_negative_tokens_fail(self):
        errors = ingest_handler.validate_event(make_valid_event(input_tokens=-1))
        assert any("input_tokens" in e for e in errors)

    def test_invalid_timestamp_fails(self):
        errors = ingest_handler.validate_event(
            make_valid_event(timestamp="not-a-date")
        )
        assert any("timestamp" in e for e in errors)

    def test_cloudwatch_backfill_source_is_valid(self):
        errors = ingest_handler.validate_event(
            make_valid_event(source="cloudwatch_backfill")
        )
        assert errors == []

    def test_null_attribution_fields_are_valid(self):
        """user_id, agent_id, session_id may all be null."""
        errors = ingest_handler.validate_event(
            make_valid_event(user_id=None, agent_id=None, session_id=None)
        )
        assert errors == []


# ── Cost compute tests ────────────────────────────────────────────────────────

class TestCostCompute:
    """Tests for the cost computation logic."""

    def test_basic_cost_calculation(self):
        prices = {
            "input": Decimal("0.00300"),
            "output": Decimal("0.01500"),
        }
        # 1000 input tokens = $0.003, 500 output tokens = $0.0075 → total $0.0105
        cost = cost_compute_handler.compute_cost(1000, 500, 0, 0, prices)
        assert cost == Decimal("0.01050000")

    def test_cache_tokens_included(self):
        prices = {
            "input": Decimal("0.00300"),
            "output": Decimal("0.01500"),
            "cache_read": Decimal("0.00030"),
            "cache_write": Decimal("0.00375"),
        }
        # 1000 cache_read = $0.0003, 1000 cache_write = $0.00375 → total $0.00405
        cost = cost_compute_handler.compute_cost(
            input_tokens=0,
            output_tokens=0,
            cache_read_tokens=1000,
            cache_write_tokens=1000,
            prices=prices,
        )
        assert cost == Decimal("0.00405000")

    def test_zero_tokens_gives_zero_cost(self):
        prices = {"input": Decimal("0.003"), "output": Decimal("0.015")}
        cost = cost_compute_handler.compute_cost(0, 0, 0, 0, prices)
        assert cost == Decimal("0")

    def test_missing_price_types_default_to_zero(self):
        """cache_read/write absent from prices dict should not raise — treat as $0."""
        prices = {"input": Decimal("0.003"), "output": Decimal("0.015")}
        # 100 input + 50 output + 200 cache_read (no price → $0)
        cost = cost_compute_handler.compute_cost(100, 50, 200, 0, prices)
        expected = (
            Decimal("0.003") * Decimal("100") / Decimal("1000")
            + Decimal("0.015") * Decimal("50") / Decimal("1000")
        ).quantize(Decimal("0.00000001"))
        assert cost == expected

    def test_result_has_at_most_8_decimal_places(self):
        prices = {"input": Decimal("0.00030"), "output": Decimal("0.01500")}
        cost = cost_compute_handler.compute_cost(1, 1, 0, 0, prices)
        decimal_places = len(str(cost).split(".")[-1])
        assert decimal_places <= 8


# ── Python wrapper tests ──────────────────────────────────────────────────────

class TestPythonWrapper:
    """Tests for the Python SDK wrapper."""

    BedrockCostWrapper = bedrock_wrapper.BedrockCostWrapper
    WrappedBedrockClient = bedrock_wrapper.WrappedBedrockClient

    def _make_wrapper(self, **kwargs):
        defaults = dict(
            ingest_api_url="https://example.com/events",
            api_key="test-key",
            application_id="test-app",
            account_id="123456789012",
            emit_async=False,  # synchronous so tests don't spin up background threads
        )
        defaults.update(kwargs)
        return self.BedrockCostWrapper(**defaults)

    def test_wrapper_initialises(self):
        wrapper = self._make_wrapper()
        assert wrapper.application_id == "test-app"

    def test_attribution_kwargs_are_stripped_before_boto3_call(self):
        """_user_id, _agent_id, _session_id must not reach boto3."""
        mock_client = MagicMock()
        wrapper = self._make_wrapper()
        wrapped = self.WrappedBedrockClient(mock_client, wrapper, "eu-central-1")

        mock_client.converse.return_value = {
            "ResponseMetadata": {"RequestId": "req_123"},
            "usage": {"inputTokens": 10, "outputTokens": 5},
            "output": {"message": {"content": [{"text": "Hello"}]}},
        }

        with patch.object(wrapper, "_send_event"):
            wrapped.converse(
                modelId="anthropic.claude-sonnet-4-5",
                messages=[{"role": "user", "content": [{"text": "hi"}]}],
                _user_id="user_123",
                _agent_id="agent_abc",
                _session_id="sess_xyz",
            )

        call_kwargs = mock_client.converse.call_args[1]
        assert "_user_id" not in call_kwargs
        assert "_agent_id" not in call_kwargs
        assert "_session_id" not in call_kwargs
        assert call_kwargs["modelId"] == "anthropic.claude-sonnet-4-5"

    def test_emit_event_never_raises_on_network_error(self):
        """A failed HTTP POST must never propagate to the caller."""
        wrapper = self._make_wrapper(emit_async=False)

        with patch.object(wrapper, "_send_event", side_effect=Exception("network down")):
            try:
                wrapper.emit_event({"event_id": "test-123", "model_id": "x"})
            except Exception:
                pytest.fail(
                    "emit_event raised an exception — it must swallow all emission errors"
                )


# ── Event schema tests ────────────────────────────────────────────────────────

class TestEventSchema:
    """Verify the event schema is complete and self-consistent."""

    REQUIRED_FIELDS = {
        "event_id", "schema_version", "timestamp", "region",
        "account_id", "model_id", "invocation_type",
        "input_tokens", "output_tokens", "status", "source",
    }

    def test_all_required_fields_present(self):
        event = make_valid_event()
        assert self.REQUIRED_FIELDS.issubset(set(event.keys()))

    def test_all_invocation_types_pass_validation(self):
        for t in ("invoke_model", "converse", "agent_step"):
            event = make_valid_event(invocation_type=t)
            errors = ingest_handler.validate_event(event)
            assert errors == [], f"invocation_type={t!r} should be valid, got: {errors}"

    def test_schema_version_is_integer(self):
        assert isinstance(make_valid_event()["schema_version"], int)
