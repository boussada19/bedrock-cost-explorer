"""
Bedrock Cost Explorer — Python instrumentation wrapper.

Wraps boto3 Bedrock runtime calls (invoke_model, converse) to capture
token usage, cost attribution metadata, and invocation context, then
ships the event to the Cost Explorer ingest API.

Usage:

    from bedrock_cost_wrapper import BedrockCostWrapper

    wrapper = BedrockCostWrapper(
        ingest_api_url="https://xxx.execute-api.us-east-1.amazonaws.com/v1/events",
        api_key="your-api-key",
        application_id="my-search-service",
    )

    # Option A: create a pre-configured boto3 client
    bedrock = wrapper.client(region_name="us-east-1")
    response = bedrock.converse(
        modelId="anthropic.claude-sonnet-4-5",
        messages=[{"role": "user", "content": [{"text": "Hello"}]}],
        # Pass attribution via additional keyword arguments — stripped before the call
        _user_id="user_abc123",
        _agent_id=None,
        _session_id="sess_xyz",
    )

    # Option B: wrap an existing client
    import boto3
    existing_client = boto3.client("bedrock-runtime", region_name="us-east-1")
    bedrock = wrapper.wrap(existing_client)

Design intent:
- The wrapper intercepts the RESPONSE to read token counts.
  It does NOT proxy or inspect the request payload (privacy-first).
- Attribution fields are passed via underscore-prefixed kwargs (_user_id etc.)
  so they don't pollute the boto3 call signature.
- Events are emitted asynchronously (background thread) to add zero latency
  to the Bedrock call itself.
- Failures in event emission are logged but never raise — the wrapper must
  never break the application.
"""

import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Callable, Dict, Optional
from urllib import request, error as urllib_error

import boto3
import botocore

logger = logging.getLogger(__name__)


class BedrockCostWrapper:
    """
    Instrumentation wrapper for boto3 Bedrock Runtime clients.

    Parameters
    ----------
    ingest_api_url : str
        URL of the Cost Explorer ingest endpoint (POST /events).
    api_key : str
        API key for the ingest endpoint.
    application_id : str
        Logical name of the application using Bedrock.
    account_id : str, optional
        AWS account ID. Auto-detected from STS if not provided.
    emit_async : bool
        If True (default), emit events in a background thread.
        Set to False in tests or Lambda environments where the process
        may exit before the background thread completes.
    max_retries : int
        Number of times to retry failed event emission. Default: 2.
    timeout_seconds : float
        HTTP timeout for event emission. Default: 5.0.
    """

    ATTRIBUTION_KWARGS = {"_user_id", "_agent_id", "_session_id"}

    def __init__(
        self,
        ingest_api_url: str,
        api_key: str,
        application_id: str,
        account_id: Optional[str] = None,
        emit_async: bool = True,
        max_retries: int = 2,
        timeout_seconds: float = 5.0,
    ):
        self.ingest_api_url = ingest_api_url.rstrip("/")
        self.api_key = api_key
        self.application_id = application_id
        self._account_id = account_id
        self.emit_async = emit_async
        self.max_retries = max_retries
        self.timeout_seconds = timeout_seconds
        self._account_id_lock = threading.Lock()

    def _get_account_id(self) -> str:
        """Lazy-fetch account ID from STS (cached after first call)."""
        if self._account_id:
            return self._account_id
        with self._account_id_lock:
            if not self._account_id:
                try:
                    sts = boto3.client("sts")
                    self._account_id = sts.get_caller_identity()["Account"]
                except Exception as exc:
                    logger.warning("Could not fetch account_id from STS: %s", exc)
                    self._account_id = "unknown"
        return self._account_id

    def client(self, **kwargs) -> "WrappedBedrockClient":
        """
        Create a new boto3 bedrock-runtime client, wrapped for instrumentation.

        All kwargs are passed through to boto3.client().
        """
        region = kwargs.get("region_name", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
        raw_client = boto3.client("bedrock-runtime", **kwargs)
        return WrappedBedrockClient(raw_client, self, region)

    def wrap(self, bedrock_client: Any) -> "WrappedBedrockClient":
        """Wrap an existing boto3 bedrock-runtime client."""
        region = bedrock_client.meta.region_name
        return WrappedBedrockClient(bedrock_client, self, region)

    def emit_event(self, event: dict):
        """
        Emit an invocation event to the ingest API.
        Called from WrappedBedrockClient after each invocation.

        NEVER raises — emission failures are always logged and swallowed
        so they cannot break the caller's Bedrock call.
        """
        if self.emit_async:
            t = threading.Thread(target=self._send_event, args=(event,), daemon=True)
            t.start()
        else:
            try:
                self._send_event(event)
            except Exception as exc:
                logger.error(
                    "Failed to emit event %s (sync): %s",
                    event.get("event_id"),
                    exc,
                )

    def _send_event(self, event: dict):
        """HTTP POST with retry. Failures are logged, never raised."""
        payload = json.dumps(event, default=str).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "X-Api-Key": self.api_key,
        }

        for attempt in range(self.max_retries + 1):
            try:
                req = request.Request(
                    self.ingest_api_url,
                    data=payload,
                    headers=headers,
                    method="POST",
                )
                with request.urlopen(req, timeout=self.timeout_seconds) as resp:
                    if resp.status not in (200, 201, 202):
                        body = resp.read().decode("utf-8", errors="replace")
                        logger.warning(
                            "Ingest API returned %d: %s (event_id=%s)",
                            resp.status, body, event.get("event_id"),
                        )
                    else:
                        logger.debug("Event %s emitted successfully", event.get("event_id"))
                    return
            except urllib_error.URLError as exc:
                if attempt < self.max_retries:
                    backoff = 0.2 * (2 ** attempt)
                    logger.debug(
                        "Emit attempt %d failed (%s) — retrying in %.1fs",
                        attempt + 1, exc, backoff,
                    )
                    time.sleep(backoff)
                else:
                    logger.error(
                        "Failed to emit event %s after %d attempts: %s",
                        event.get("event_id"), self.max_retries + 1, exc,
                    )
            except Exception as exc:
                logger.error("Unexpected error emitting event %s: %s", event.get("event_id"), exc)
                return


class WrappedBedrockClient:
    """
    Thin wrapper around a boto3 bedrock-runtime client.

    Intercepts invoke_model and converse calls, extracts token usage
    from the response, and emits instrumentation events.

    Attribution is passed as underscore-prefixed kwargs:
        _user_id, _agent_id, _session_id

    These are stripped before the call reaches boto3.
    """

    def __init__(self, client: Any, wrapper: BedrockCostWrapper, region: str):
        self._client = client
        self._wrapper = wrapper
        self._region = region

    def _extract_attribution(self, kwargs: dict) -> dict:
        """Pop attribution kwargs and return them."""
        return {
            "user_id": kwargs.pop("_user_id", None),
            "agent_id": kwargs.pop("_agent_id", None),
            "session_id": kwargs.pop("_session_id", None),
        }

    def _build_event(
        self,
        invocation_type: str,
        model_id: str,
        attribution: dict,
        response: dict,
        latency_ms: float,
        status: str,
        error_code: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> dict:
        """Build the canonical invocation event from a Bedrock response."""

        # Extract token counts — location differs by API
        usage = {}
        if invocation_type == "converse":
            usage = response.get("usage", {})
            input_tokens = usage.get("inputTokens", 0)
            output_tokens = usage.get("outputTokens", 0)
            cache_read = usage.get("cacheReadInputTokens", 0)
            cache_write = usage.get("cacheWriteInputTokens", 0)
        elif invocation_type == "invoke_model":
            # Claude models return usage in the response body JSON
            body = response.get("body", b"")
            if hasattr(body, "read"):
                body = body.read()
            try:
                parsed = json.loads(body)
                usage = parsed.get("usage", {})
                input_tokens = usage.get("input_tokens", 0)
                output_tokens = usage.get("output_tokens", 0)
                cache_read = usage.get("cache_read_input_tokens", 0)
                cache_write = usage.get("cache_creation_input_tokens", 0)
            except (json.JSONDecodeError, Exception):
                input_tokens = output_tokens = cache_read = cache_write = 0
        else:
            input_tokens = output_tokens = cache_read = cache_write = 0

        return {
            "event_id": str(uuid.uuid4()),
            "schema_version": 1,
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "region": self._region,
            "account_id": self._wrapper._get_account_id(),
            "model_id": model_id,
            "invocation_type": invocation_type,
            "user_id": attribution.get("user_id"),
            "agent_id": attribution.get("agent_id"),
            "session_id": attribution.get("session_id"),
            "application_id": self._wrapper.application_id,
            "request_id": request_id,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cache_read_tokens": cache_read,
            "cache_write_tokens": cache_write,
            "latency_ms": int(latency_ms),
            "status": status,
            "error_code": error_code,
            "source": "wrapper",
        }

    def converse(self, **kwargs) -> dict:
        """
        Instrumented wrapper for bedrock-runtime.converse().

        Attribution kwargs: _user_id, _agent_id, _session_id
        All other kwargs are passed through to the boto3 client.
        """
        attribution = self._extract_attribution(kwargs)
        model_id = kwargs.get("modelId", "unknown")

        start = time.monotonic()
        status = "success"
        error_code = None
        response = {}

        try:
            response = self._client.converse(**kwargs)
            return response
        except botocore.exceptions.ClientError as exc:
            status = "error"
            error_code = exc.response.get("Error", {}).get("Code", "UnknownError")
            if error_code == "ThrottlingException":
                status = "throttled"
            raise
        finally:
            latency_ms = (time.monotonic() - start) * 1000
            request_id = (
                response.get("ResponseMetadata", {}).get("RequestId")
                if response
                else None
            )
            event = self._build_event(
                invocation_type="converse",
                model_id=model_id,
                attribution=attribution,
                response=response,
                latency_ms=latency_ms,
                status=status,
                error_code=error_code,
                request_id=request_id,
            )
            self._wrapper.emit_event(event)

    def invoke_model(self, **kwargs) -> dict:
        """
        Instrumented wrapper for bedrock-runtime.invoke_model().

        Attribution kwargs: _user_id, _agent_id, _session_id
        All other kwargs are passed through to the boto3 client.
        """
        attribution = self._extract_attribution(kwargs)
        model_id = kwargs.get("modelId", "unknown")

        start = time.monotonic()
        status = "success"
        error_code = None
        response = {}

        try:
            response = self._client.invoke_model(**kwargs)
            return response
        except botocore.exceptions.ClientError as exc:
            status = "error"
            error_code = exc.response.get("Error", {}).get("Code", "UnknownError")
            if error_code == "ThrottlingException":
                status = "throttled"
            raise
        finally:
            latency_ms = (time.monotonic() - start) * 1000
            request_id = (
                response.get("ResponseMetadata", {}).get("RequestId")
                if response
                else None
            )
            event = self._build_event(
                invocation_type="invoke_model",
                model_id=model_id,
                attribution=attribution,
                response=response,
                latency_ms=latency_ms,
                status=status,
                error_code=error_code,
                request_id=request_id,
            )
            self._wrapper.emit_event(event)

    def __getattr__(self, name: str):
        """Pass through any other client methods uninstrumented."""
        return getattr(self._client, name)


# ──────────────────────────────────────────────
# Bedrock Agents step interceptor
# ──────────────────────────────────────────────

class AgentStepInterceptor:
    """
    Captures token usage from Bedrock Agent invocation responses.

    Bedrock Agents don't expose per-step token counts directly via the
    InvokeAgent API. This interceptor wraps invoke_agent and parses the
    response stream to accumulate token usage across steps.

    Usage:
        interceptor = AgentStepInterceptor(wrapper, agent_id="XXXXXXXXXX")
        response = interceptor.invoke_agent(
            agentId="XXXXXXXXXX",
            agentAliasId="YYYYYYYYYY",
            sessionId="sess_123",
            inputText="What is my account balance?",
            _user_id="user_abc",
        )
    """

    def __init__(
        self,
        wrapper: BedrockCostWrapper,
        region: Optional[str] = None,
    ):
        self._wrapper = wrapper
        self._region = region or os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
        self._agents_client = boto3.client(
            "bedrock-agent-runtime", region_name=self._region
        )

    def invoke_agent(self, **kwargs) -> dict:
        """
        Instrumented invoke_agent. Reads the response event stream and
        accumulates token usage from 'metadata' event chunks.
        """
        user_id = kwargs.pop("_user_id", None)
        agent_id = kwargs.get("agentId", "unknown")
        session_id = kwargs.get("sessionId")
        model_id = f"agent/{agent_id}"

        start = time.monotonic()
        input_tokens = output_tokens = 0
        status = "success"
        error_code = None

        try:
            response = self._agents_client.invoke_agent(**kwargs)
            event_stream = response.get("completion", [])

            # Drain the stream — must consume to get token counts
            collected_chunks = []
            for stream_event in event_stream:
                collected_chunks.append(stream_event)

                # Bedrock Agents emit a 'metadata' event per step with usage
                if "metadata" in stream_event:
                    usage = stream_event["metadata"].get("usage", {})
                    input_tokens += usage.get("inputTokens", 0)
                    output_tokens += usage.get("outputTokens", 0)

            # Rebuild a response object with the drained stream
            response["_collected_chunks"] = collected_chunks
            return response

        except botocore.exceptions.ClientError as exc:
            status = "error"
            error_code = exc.response.get("Error", {}).get("Code", "UnknownError")
            raise
        finally:
            latency_ms = (time.monotonic() - start) * 1000
            event = {
                "event_id": str(uuid.uuid4()),
                "schema_version": 1,
                "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "region": self._region,
                "account_id": self._wrapper._get_account_id(),
                "model_id": model_id,
                "invocation_type": "agent_step",
                "user_id": user_id,
                "agent_id": agent_id,
                "session_id": session_id,
                "application_id": self._wrapper.application_id,
                "request_id": None,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cache_read_tokens": 0,
                "cache_write_tokens": 0,
                "latency_ms": int(latency_ms),
                "status": status,
                "error_code": error_code,
                "source": "wrapper",
            }
            self._wrapper.emit_event(event)
