# Bedrock Cost Explorer

Real-time cost visibility for AWS Bedrock invocations. Costs are **computed from token counts × a versioned price table** — never from billing pipeline data, which lags ~24h.

## Architecture

```
Your App
  └── SDK Wrapper (Python / TypeScript)
        └── POST /events  →  API Gateway  →  Ingest Lambda
                                                  └── DynamoDB (events table)
                                                  └── SQS  →  Cost Compute Lambda
                                                                └── DynamoDB (enriches event with computed_cost_usd)

CloudWatch Logs / Bedrock Model Invocation Logs
  └── Backfill Lambda (scheduled, catches wrapper bypasses)
        └── DynamoDB (source=cloudwatch_backfill)

AWS CUR (S3, daily)
  └── Reconcile Lambda (scheduled daily)
        └── DynamoDB (reconciliation_runs table)
        └── Variance report → SNS

Dashboard API
  └── Query Lambda  →  DynamoDB GSIs  →  JSON  →  React Dashboard
```

## Key design constraints

1. **Real-time cost is COMPUTED, not billed.** `tokens × price_table` gives sub-second cost attribution. CUR is reconciliation only.
2. **The wrapper is the primary data hook.** Every Bedrock call is captured at the call site with full attribution context.
3. **CloudWatch is backfill, not the live path.** Used only to catch calls that bypass the wrapper.
4. **Price table is versioned.** Historical events keep the price that was active when they fired.
5. **Single account MVP, org-ready design.** `account_id` is a first-class field everywhere.

## Directory structure

```
infrastructure/         CDK stack (all AWS resources)
lambdas/
  ingest/               Receives events from SDK wrappers
  cost_compute/         Enriches events with computed cost
  backfill/             Pulls from CloudWatch, fills gaps
  reconcile/            Daily CUR vs computed-cost diff
  query_api/            Read-layer for dashboard
sdk/
  python/               Python wrapper for Bedrock calls
  typescript/           TypeScript wrapper for Bedrock calls
price_table/            Seed data + update scripts
dashboard/              React dashboard app
scripts/                Operational scripts (backfill, price updates)
```

## Setup

```bash
# 1. Install CDK dependencies
cd infrastructure && npm install

# 2. Seed price table
cd ../scripts && python seed_prices.py

# 3. Deploy infrastructure
cd ../infrastructure && cdk deploy

# 4. Configure your app (Python example)
pip install ./sdk/python
```

See each subdirectory for component-specific documentation.
