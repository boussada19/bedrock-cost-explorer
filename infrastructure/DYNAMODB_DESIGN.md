# DynamoDB Table Design

## Tables

### 1. `bedrock_events`

Primary data store for every Bedrock invocation event.

**Access patterns:**
- Get single event by ID (operational / debug)
- List events by time range (dashboard, global)
- List events by agent_id + time (per-agent cost)
- List events by user_id + time (per-user cost)
- List events by application_id + time (per-app cost)
- List events by model_id + time (per-model analytics)
- Aggregate cost/tokens by any dimension over a time window

**Schema:**

| Attribute | Type | Notes |
|-----------|------|-------|
| PK | S | `EVENT#<event_id>` |
| SK | S | `<timestamp_iso>` (ISO 8601, sortable) |
| event_id | S | UUID v7 (time-ordered) |
| schema_version | N | 1 |
| timestamp | S | ISO 8601 UTC ms |
| region | S | e.g. `us-east-1` |
| account_id | S | 12-digit AWS account |
| model_id | S | exact Bedrock model string |
| invocation_type | S | `invoke_model` \| `converse` \| `agent_step` |
| user_id | S | nullable → stored as `NULL` sentinel |
| agent_id | S | nullable |
| session_id | S | nullable |
| application_id | S | nullable |
| request_id | S | x-amzn-requestid header |
| input_tokens | N | |
| output_tokens | N | |
| cache_read_tokens | N | default 0 |
| cache_write_tokens | N | default 0 |
| computed_cost_usd | N | null until enriched by cost_compute |
| price_table_version | N | null until enriched |
| latency_ms | N | |
| status | S | `success` \| `error` \| `throttled` |
| error_code | S | nullable |
| source | S | `wrapper` \| `cloudwatch_backfill` |
| ttl | N | Unix epoch; optional data retention |

**GSIs:**

| GSI | PK | SK | Purpose |
|-----|----|----|---------|
| `gsi_agent_time` | `agent_id` | `timestamp` | Per-agent queries |
| `gsi_user_time` | `user_id` | `timestamp` | Per-user queries |
| `gsi_app_time` | `application_id` | `timestamp` | Per-app queries |
| `gsi_model_time` | `model_id` | `timestamp` | Per-model analytics |
| `gsi_account_time` | `account_id` | `timestamp` | Future org-wide queries |

All GSIs project `ALL` attributes to avoid extra fetches. For very high scale, switch to `KEYS_ONLY` + selective attribute projection.

---

### 2. `bedrock_price_table`

Versioned per-model pricing. Append-only; never update rows.

| Attribute | Type | Notes |
|-----------|------|-------|
| PK | S | `VERSION#<version_id>` |
| SK | S | `<model_id>#<region>#<price_type>` |
| version_id | N | monotonic integer |
| effective_from | S | ISO 8601 UTC |
| effective_until | S | ISO 8601 UTC or `9999-12-31T23:59:59Z` (active) |
| model_id | S | exact Bedrock model string |
| region | S | e.g. `us-east-1` or `*` (wildcard) |
| price_type | S | `input` \| `output` \| `cache_read` \| `cache_write` |
| price_per_1k_tokens | N | USD, 8 decimal places |
| notes | S | e.g. "AWS price drop 2025-07-01" |

**GSI:**

| GSI | PK | SK | Purpose |
|-----|----|----|---------|
| `gsi_active_prices` | `effective_until` | `model_id` | Find current prices for model |

Query pattern for price lookup:
1. Query `gsi_active_prices` where `effective_until = '9999-12-31T23:59:59Z'` and filter on `model_id`
2. If no exact region match, fall back to `region = '*'`
3. For historical event enrichment, load all version ranges and binary-search on `effective_from`

---

### 3. `bedrock_reconciliation_runs`

Records each daily CUR reconciliation run and its variance output.

| Attribute | Type | Notes |
|-----------|------|-------|
| PK | S | `RUN#<date>` e.g. `RUN#2025-08-01` |
| SK | S | `SUMMARY` or `MODEL#<model_id>` |
| run_date | S | YYYY-MM-DD |
| computed_cost_usd | N | sum of computed costs for the day |
| billed_cost_usd | N | from CUR for same period |
| variance_usd | N | `billed - computed` (positive = undercounting) |
| variance_pct | N | `(variance / billed) * 100` |
| event_count | N | number of events in window |
| unmatched_cw_invocations | N | CW invocations not found in events |
| coverage_pct | N | `wrapper_events / total_cw_invocations * 100` |
| notes | S | flags, e.g. "Savings Plan discount detected" |
| created_at | S | ISO 8601 |

---

### Capacity & cost notes (MVP)

- **Billing mode**: PAY_PER_REQUEST for all tables (auto-scales, no capacity planning needed at low-medium volume)
- **Data retention**: Set TTL on `bedrock_events` to 90 days for MVP; export to S3 for longer retention
- **Migration path to analytics DB**: At ~1M events/day, export DynamoDB streams to S3 via Kinesis Firehose, then query with Athena or load into ClickHouse. The event schema is stable; no application changes needed.
