"""
Example: Instrumenting a Python application with the Bedrock Cost Wrapper.

This shows both the simple (converse) and direct (invoke_model) patterns,
plus the Agents interceptor.
"""

import os
from bedrock_cost_wrapper import BedrockCostWrapper, AgentStepInterceptor

# ── One-time setup (at app startup or module level) ──────────────────────────

wrapper = BedrockCostWrapper(
    ingest_api_url=os.environ["BEDROCK_COST_INGEST_URL"],
    api_key=os.environ["BEDROCK_COST_API_KEY"],
    application_id="my-search-service",  # logical app name
    # account_id is auto-detected from STS if omitted
    emit_async=True,    # background thread; zero added latency to Bedrock calls
    max_retries=2,
    timeout_seconds=5.0,
)

# Create an instrumented client (drop-in for boto3.client("bedrock-runtime"))
bedrock = wrapper.client(region_name="us-east-1")


# ── Pattern A: Converse API ───────────────────────────────────────────────────

def answer_question(question: str, user_id: str, session_id: str) -> str:
    """Use the Converse API with full attribution."""
    response = bedrock.converse(
        modelId="anthropic.claude-sonnet-4-5",
        messages=[
            {"role": "user", "content": [{"text": question}]}
        ],
        system=[{"text": "You are a helpful assistant."}],
        # Attribution kwargs — stripped before reaching boto3
        _user_id=user_id,
        _agent_id=None,
        _session_id=session_id,
    )
    return response["output"]["message"]["content"][0]["text"]


# ── Pattern B: InvokeModel API ───────────────────────────────────────────────

import json

def classify_text(text: str, user_id: str) -> dict:
    """Use the InvokeModel API directly."""
    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 256,
        "messages": [
            {"role": "user", "content": text}
        ],
    })

    response = bedrock.invoke_model(
        modelId="anthropic.claude-haiku-3-5",
        body=body,
        contentType="application/json",
        accept="application/json",
        # Attribution
        _user_id=user_id,
        _session_id=None,
    )

    result = json.loads(response["body"].read())
    return result


# ── Pattern C: Bedrock Agents ─────────────────────────────────────────────────

agent_interceptor = AgentStepInterceptor(
    wrapper=wrapper,
    region="us-east-1",
)

def run_agent(user_input: str, user_id: str, session_id: str) -> str:
    """Invoke a Bedrock Agent with token tracking across all steps."""
    response = agent_interceptor.invoke_agent(
        agentId=os.environ["BEDROCK_AGENT_ID"],
        agentAliasId=os.environ["BEDROCK_AGENT_ALIAS_ID"],
        sessionId=session_id,
        inputText=user_input,
        # Attribution
        _user_id=user_id,
    )

    # The agent response stream has been drained; chunks are in _collected_chunks
    output_parts = []
    for chunk in response.get("_collected_chunks", []):
        if "chunk" in chunk and "bytes" in chunk["chunk"]:
            output_parts.append(chunk["chunk"]["bytes"].decode("utf-8"))

    return "".join(output_parts)


# ── Pattern D: Wrapping an existing boto3 client ──────────────────────────────

import boto3

existing_client = boto3.client(
    "bedrock-runtime",
    region_name="us-west-2",
    # your existing config...
)

# Wrap it — same interface, adds instrumentation
instrumented = wrapper.wrap(existing_client)


# ── Running examples ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    import dotenv
    dotenv.load_dotenv()

    # Example A
    print("Testing Converse API...")
    answer = answer_question(
        question="What is the capital of France?",
        user_id="user_demo_001",
        session_id="sess_example_abc",
    )
    print(f"Answer: {answer[:100]}...")

    # Example B
    print("\nTesting InvokeModel API...")
    result = classify_text(
        text="Classify this as positive or negative: 'Great product!'",
        user_id="user_demo_001",
    )
    print(f"Classification result keys: {list(result.keys())}")

    print("\nDone. Check the Bedrock Cost Explorer dashboard for the events.")
