/**
 * Bedrock Cost Explorer — TypeScript instrumentation wrapper.
 *
 * Wraps @aws-sdk/client-bedrock-runtime calls (InvokeModel, Converse)
 * to capture token usage and attribution metadata, then ships the event
 * to the Cost Explorer ingest API.
 *
 * Usage:
 *
 *   import { BedrockCostWrapper } from "./bedrock-cost-wrapper";
 *
 *   const wrapper = new BedrockCostWrapper({
 *     ingestApiUrl: "https://xxx.execute-api.us-east-1.amazonaws.com/v1/events",
 *     apiKey: process.env.BEDROCK_COST_API_KEY!,
 *     applicationId: "my-search-service",
 *   });
 *
 *   const bedrock = wrapper.client({ region: "us-east-1" });
 *
 *   const response = await bedrock.converse({
 *     modelId: "anthropic.claude-sonnet-4-5",
 *     messages: [{ role: "user", content: [{ text: "Hello" }] }],
 *     // Attribution — stripped before the SDK call
 *     _userId: "user_abc123",
 *     _agentId: undefined,
 *     _sessionId: "sess_xyz",
 *   });
 */

import {
  BedrockRuntimeClient,
  BedrockRuntimeClientConfig,
  ConverseCommand,
  ConverseCommandInput,
  ConverseCommandOutput,
  InvokeModelCommand,
  InvokeModelCommandInput,
  InvokeModelCommandOutput,
} from "@aws-sdk/client-bedrock-runtime";
import { STSClient, GetCallerIdentityCommand } from "@aws-sdk/client-sts";
import { randomUUID } from "crypto";
import { fetch } from "undici"; // Node 18+ built-in fetch fallback

// ─────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────

export interface WrapperConfig {
  ingestApiUrl: string;
  apiKey: string;
  applicationId: string;
  accountId?: string;
  /** Emit events asynchronously (default: true). Set false in tests. */
  emitAsync?: boolean;
  maxRetries?: number;
  timeoutMs?: number;
}

export interface AttributionOptions {
  _userId?: string | null;
  _agentId?: string | null;
  _sessionId?: string | null;
}

export interface InvocationEvent {
  event_id: string;
  schema_version: number;
  timestamp: string;
  region: string;
  account_id: string;
  model_id: string;
  invocation_type: "invoke_model" | "converse" | "agent_step";
  user_id: string | null;
  agent_id: string | null;
  session_id: string | null;
  application_id: string;
  request_id: string | null;
  input_tokens: number;
  output_tokens: number;
  cache_read_tokens: number;
  cache_write_tokens: number;
  latency_ms: number;
  status: "success" | "error" | "throttled";
  error_code: string | null;
  source: "wrapper" | "cloudwatch_backfill";
}

// ─────────────────────────────────────────────
// BedrockCostWrapper
// ─────────────────────────────────────────────

export class BedrockCostWrapper {
  private readonly config: Required<WrapperConfig>;
  private _accountId: string | null = null;
  private _accountIdPromise: Promise<string> | null = null;

  constructor(config: WrapperConfig) {
    this.config = {
      accountId: undefined as unknown as string,
      emitAsync: true,
      maxRetries: 2,
      timeoutMs: 5000,
      ...config,
    };
  }

  private async getAccountId(): Promise<string> {
    if (this.config.accountId) return this.config.accountId;
    if (this._accountId) return this._accountId;
    if (!this._accountIdPromise) {
      this._accountIdPromise = (async () => {
        try {
          const sts = new STSClient({});
          const resp = await sts.send(new GetCallerIdentityCommand({}));
          this._accountId = resp.Account ?? "unknown";
          return this._accountId;
        } catch {
          this._accountId = "unknown";
          return this._accountId;
        }
      })();
    }
    return this._accountIdPromise;
  }

  /** Create an instrumented BedrockRuntime client. */
  client(clientConfig: BedrockRuntimeClientConfig = {}): WrappedBedrockClient {
    const raw = new BedrockRuntimeClient(clientConfig);
    const region =
      (typeof clientConfig.region === "string" ? clientConfig.region : null) ??
      process.env.AWS_DEFAULT_REGION ??
      "us-east-1";
    return new WrappedBedrockClient(raw, this, region);
  }

  /** Wrap an existing BedrockRuntimeClient. */
  wrap(client: BedrockRuntimeClient, region?: string): WrappedBedrockClient {
    const r = region ?? process.env.AWS_DEFAULT_REGION ?? "us-east-1";
    return new WrappedBedrockClient(client, this, r);
  }

  async emitEvent(event: InvocationEvent): Promise<void> {
    // Fill in account_id async (cached after first call)
    event.account_id = await this.getAccountId();

    if (this.config.emitAsync) {
      // Fire-and-forget; errors logged, never thrown
      this.sendEvent(event).catch((err) => {
        console.error(
          `[BedrockCostWrapper] Failed to emit event ${event.event_id}:`,
          err
        );
      });
    } else {
      await this.sendEvent(event);
    }
  }

  private async sendEvent(
    event: InvocationEvent,
    attempt = 0
  ): Promise<void> {
    const controller = new AbortController();
    const timer = setTimeout(
      () => controller.abort(),
      this.config.timeoutMs
    );
    try {
      const resp = await fetch(this.config.ingestApiUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Api-Key": this.config.apiKey,
        },
        body: JSON.stringify(event),
        signal: controller.signal,
      });
      if (!resp.ok) {
        const body = await resp.text();
        console.warn(
          `[BedrockCostWrapper] Ingest API ${resp.status}: ${body}`
        );
      }
    } catch (err) {
      if (attempt < this.config.maxRetries) {
        const backoff = 200 * Math.pow(2, attempt);
        await new Promise((r) => setTimeout(r, backoff));
        return this.sendEvent(event, attempt + 1);
      }
      throw err;
    } finally {
      clearTimeout(timer);
    }
  }

  get applicationId(): string {
    return this.config.applicationId;
  }
}

// ─────────────────────────────────────────────
// WrappedBedrockClient
// ─────────────────────────────────────────────

export class WrappedBedrockClient {
  constructor(
    private readonly _client: BedrockRuntimeClient,
    private readonly _wrapper: BedrockCostWrapper,
    private readonly _region: string
  ) {}

  private extractAttribution(input: Record<string, unknown>): AttributionOptions {
    const attr: AttributionOptions = {
      _userId: (input["_userId"] as string) ?? null,
      _agentId: (input["_agentId"] as string) ?? null,
      _sessionId: (input["_sessionId"] as string) ?? null,
    };
    delete input["_userId"];
    delete input["_agentId"];
    delete input["_sessionId"];
    return attr;
  }

  async converse(
    input: ConverseCommandInput & AttributionOptions
  ): Promise<ConverseCommandOutput> {
    const mutableInput = { ...input } as Record<string, unknown>;
    const attribution = this.extractAttribution(mutableInput);
    const modelId = (mutableInput["modelId"] as string) ?? "unknown";

    const start = performance.now();
    let status: "success" | "error" | "throttled" = "success";
    let errorCode: string | null = null;
    let response: ConverseCommandOutput | null = null;

    try {
      response = await this._client.send(
        new ConverseCommand(mutableInput as ConverseCommandInput)
      );
      return response;
    } catch (err: unknown) {
      status = "error";
      const awsErr = err as { name?: string };
      errorCode = awsErr?.name ?? "UnknownError";
      if (errorCode === "ThrottlingException") status = "throttled";
      throw err;
    } finally {
      const latencyMs = performance.now() - start;
      const usage = response?.usage ?? {};

      const event: InvocationEvent = {
        event_id: randomUUID(),
        schema_version: 1,
        timestamp: new Date().toISOString(),
        region: this._region,
        account_id: "pending", // filled by emitEvent
        model_id: modelId,
        invocation_type: "converse",
        user_id: attribution._userId ?? null,
        agent_id: attribution._agentId ?? null,
        session_id: attribution._sessionId ?? null,
        application_id: this._wrapper.applicationId,
        request_id:
          (response?.$metadata?.requestId as string | undefined) ?? null,
        input_tokens: usage.inputTokens ?? 0,
        output_tokens: usage.outputTokens ?? 0,
        cache_read_tokens: (usage as Record<string, number>).cacheReadInputTokens ?? 0,
        cache_write_tokens: (usage as Record<string, number>).cacheWriteInputTokens ?? 0,
        latency_ms: Math.round(latencyMs),
        status,
        error_code: errorCode,
        source: "wrapper",
      };

      // Don't await — emit fires in background
      this._wrapper.emitEvent(event);
    }
  }

  async invokeModel(
    input: InvokeModelCommandInput & AttributionOptions
  ): Promise<InvokeModelCommandOutput> {
    const mutableInput = { ...input } as Record<string, unknown>;
    const attribution = this.extractAttribution(mutableInput);
    const modelId = (mutableInput["modelId"] as string) ?? "unknown";

    const start = performance.now();
    let status: "success" | "error" | "throttled" = "success";
    let errorCode: string | null = null;
    let response: InvokeModelCommandOutput | null = null;

    try {
      response = await this._client.send(
        new InvokeModelCommand(mutableInput as InvokeModelCommandInput)
      );
      return response;
    } catch (err: unknown) {
      status = "error";
      const awsErr = err as { name?: string };
      errorCode = awsErr?.name ?? "UnknownError";
      if (errorCode === "ThrottlingException") status = "throttled";
      throw err;
    } finally {
      const latencyMs = performance.now() - start;

      // Parse token counts from response body (Claude models)
      let inputTokens = 0;
      let outputTokens = 0;
      let cacheRead = 0;
      let cacheWrite = 0;
      if (response?.body) {
        try {
          const body = JSON.parse(
            new TextDecoder().decode(response.body as Uint8Array)
          );
          const usage = body?.usage ?? {};
          inputTokens = usage.input_tokens ?? 0;
          outputTokens = usage.output_tokens ?? 0;
          cacheRead = usage.cache_read_input_tokens ?? 0;
          cacheWrite = usage.cache_creation_input_tokens ?? 0;
        } catch {
          // Non-JSON or non-Claude model response — tokens remain 0
        }
      }

      const event: InvocationEvent = {
        event_id: randomUUID(),
        schema_version: 1,
        timestamp: new Date().toISOString(),
        region: this._region,
        account_id: "pending",
        model_id: modelId,
        invocation_type: "invoke_model",
        user_id: attribution._userId ?? null,
        agent_id: attribution._agentId ?? null,
        session_id: attribution._sessionId ?? null,
        application_id: this._wrapper.applicationId,
        request_id:
          (response?.$metadata?.requestId as string | undefined) ?? null,
        input_tokens: inputTokens,
        output_tokens: outputTokens,
        cache_read_tokens: cacheRead,
        cache_write_tokens: cacheWrite,
        latency_ms: Math.round(latencyMs),
        status,
        error_code: errorCode,
        source: "wrapper",
      };

      this._wrapper.emitEvent(event);
    }
  }

  /** Expose the underlying client for operations we don't instrument. */
  get rawClient(): BedrockRuntimeClient {
    return this._client;
  }
}
