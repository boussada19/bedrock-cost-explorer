import * as cdk from "aws-cdk-lib";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as apigateway from "aws-cdk-lib/aws-apigateway";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as iam from "aws-cdk-lib/aws-iam";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as sns from "aws-cdk-lib/aws-sns";
import * as subscriptions from "aws-cdk-lib/aws-sns-subscriptions";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as logs from "aws-cdk-lib/aws-logs";
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import { Construct } from "constructs";

export interface BedrockCostExplorerProps extends cdk.StackProps {
  alertEmail: string;
  /** Retention period for raw events in days. Default: 90 */
  eventRetentionDays?: number;
  /**
   * S3 bucket name where AWS drops the Cost and Usage Report.
   * If undefined, the reconciliation Lambda is deployed but skipped at runtime.
   */
  curBucketName?: string;
  /** Optional: tag all resources for cost allocation */
  costAllocationTags?: Record<string, string>;
}

export class BedrockCostExplorerStack extends cdk.Stack {
  /** Expose table names for cross-stack references or integration tests */
  public readonly eventsTableName: string;
  public readonly priceTableName: string;
  public readonly ingestApiUrl: string;

  constructor(scope: Construct, id: string, props: BedrockCostExplorerProps) {
    super(scope, id, props);

    const eventRetentionDays = props.eventRetentionDays ?? 90;

    // ─────────────────────────────────────────────
    // DYNAMODB TABLES
    // ─────────────────────────────────────────────

    const eventsTable = new dynamodb.Table(this, "BedrockEventsTable", {
      tableName: "bedrock_events",
      partitionKey: { name: "PK", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "SK", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      timeToLiveAttribute: "ttl",
      pointInTimeRecovery: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      stream: dynamodb.StreamViewType.NEW_IMAGE, // for future Kinesis Firehose export
    });

    // GSIs for dashboard query patterns
    const gsiConfig = [
      { name: "gsi_agent_time", pk: "agent_id" },
      { name: "gsi_user_time", pk: "user_id" },
      { name: "gsi_app_time", pk: "application_id" },
      { name: "gsi_model_time", pk: "model_id" },
      { name: "gsi_account_time", pk: "account_id" }, // org-ready
    ];
    for (const gsi of gsiConfig) {
      eventsTable.addGlobalSecondaryIndex({
        indexName: gsi.name,
        partitionKey: { name: gsi.pk, type: dynamodb.AttributeType.STRING },
        sortKey: { name: "timestamp", type: dynamodb.AttributeType.STRING },
        projectionType: dynamodb.ProjectionType.ALL,
      });
    }

    const priceTable = new dynamodb.Table(this, "BedrockPriceTable", {
      tableName: "bedrock_price_table",
      partitionKey: { name: "PK", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "SK", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });
    priceTable.addGlobalSecondaryIndex({
      indexName: "gsi_active_prices",
      partitionKey: {
        name: "effective_until",
        type: dynamodb.AttributeType.STRING,
      },
      sortKey: { name: "model_id", type: dynamodb.AttributeType.STRING },
      projectionType: dynamodb.ProjectionType.ALL,
    });

    const reconciliationTable = new dynamodb.Table(
      this,
      "BedrockReconciliationTable",
      {
        tableName: "bedrock_reconciliation_runs",
        partitionKey: { name: "PK", type: dynamodb.AttributeType.STRING },
        sortKey: { name: "SK", type: dynamodb.AttributeType.STRING },
        billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
        removalPolicy: cdk.RemovalPolicy.RETAIN,
      }
    );

    // ─────────────────────────────────────────────
    // SQS: async cost enrichment queue
    // ─────────────────────────────────────────────

    const costEnrichmentDlq = new sqs.Queue(this, "CostEnrichmentDlq", {
      queueName: "bedrock-cost-enrichment-dlq",
      retentionPeriod: cdk.Duration.days(14),
    });

    const costEnrichmentQueue = new sqs.Queue(this, "CostEnrichmentQueue", {
      queueName: "bedrock-cost-enrichment",
      visibilityTimeout: cdk.Duration.seconds(30),
      deadLetterQueue: {
        queue: costEnrichmentDlq,
        maxReceiveCount: 3,
      },
    });

    // ─────────────────────────────────────────────
    // SNS: alerts
    // ─────────────────────────────────────────────

    const alertTopic = new sns.Topic(this, "BedrockCostAlerts", {
      topicName: "bedrock-cost-alerts",
      displayName: "Bedrock Cost Explorer Alerts",
    });
    alertTopic.addSubscription(
      new subscriptions.EmailSubscription(props.alertEmail)
    );

    // ─────────────────────────────────────────────
    // LAMBDA: common environment & layer
    // ─────────────────────────────────────────────

    const commonEnv: Record<string, string> = {
      EVENTS_TABLE: eventsTable.tableName,
      PRICE_TABLE: priceTable.tableName,
      RECONCILIATION_TABLE: reconciliationTable.tableName,
      COST_ENRICHMENT_QUEUE_URL: costEnrichmentQueue.queueUrl,
      ALERT_TOPIC_ARN: alertTopic.topicArn,
      EVENT_RETENTION_DAYS: String(eventRetentionDays),
      POWERTOOLS_SERVICE_NAME: "bedrock-cost-explorer",
      LOG_LEVEL: "INFO",
    };

    if (props.curBucketName) {
      commonEnv["CUR_BUCKET"] = props.curBucketName;
    }

    const lambdaDefaults: Partial<lambda.FunctionProps> = {
      runtime: lambda.Runtime.PYTHON_3_12,
      memorySize: 256,
      timeout: cdk.Duration.seconds(30),
      logRetention: logs.RetentionDays.ONE_MONTH,
      environment: commonEnv,
    };

    // ─────────────────────────────────────────────
    // LAMBDA: ingest (hot path — keep it lean)
    // ─────────────────────────────────────────────

    const ingestLambda = new lambda.Function(this, "IngestLambda", {
      ...lambdaDefaults,
      functionName: "bedrock-cost-ingest",
      code: lambda.Code.fromAsset("../lambdas/ingest"),
      handler: "handler.lambda_handler",
      memorySize: 128, // ingest is lightweight; write + queue only
      timeout: cdk.Duration.seconds(10),
    });
    eventsTable.grantWriteData(ingestLambda);
    costEnrichmentQueue.grantSendMessages(ingestLambda);

    // ─────────────────────────────────────────────
    // LAMBDA: cost compute (enriches events async)
    // ─────────────────────────────────────────────

    const costComputeLambda = new lambda.Function(this, "CostComputeLambda", {
      ...lambdaDefaults,
      functionName: "bedrock-cost-compute",
      code: lambda.Code.fromAsset("../lambdas/cost_compute"),
      handler: "handler.lambda_handler",
      reservedConcurrentExecutions: 10, // throttle to protect DynamoDB write capacity
    });
    eventsTable.grantReadWriteData(costComputeLambda);
    priceTable.grantReadData(costComputeLambda);
    costComputeLambda.addEventSource(
      new SqsEventSource(costEnrichmentQueue, {
        batchSize: 10,
        maxBatchingWindow: cdk.Duration.seconds(5),
      })
    );

    // ─────────────────────────────────────────────
    // LAMBDA: backfill (CloudWatch secondary path)
    // ─────────────────────────────────────────────

    const backfillLambda = new lambda.Function(this, "BackfillLambda", {
      ...lambdaDefaults,
      functionName: "bedrock-cost-backfill",
      code: lambda.Code.fromAsset("../lambdas/backfill"),
      handler: "handler.lambda_handler",
      timeout: cdk.Duration.minutes(5),
      memorySize: 512,
    });
    eventsTable.grantReadWriteData(backfillLambda);
    priceTable.grantReadData(backfillLambda);
    costEnrichmentQueue.grantSendMessages(backfillLambda);
    backfillLambda.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          "logs:FilterLogEvents",
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams",
        ],
        resources: ["*"], // CloudWatch Logs for Bedrock model invocation logs
      })
    );
    backfillLambda.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["bedrock:GetModelInvocationLoggingConfiguration"],
        resources: ["*"],
      })
    );

    // Backfill runs every hour to catch missed events
    const backfillRule = new events.Rule(this, "BackfillSchedule", {
      schedule: events.Schedule.rate(cdk.Duration.hours(1)),
      description: "Trigger backfill Lambda to reconcile CloudWatch vs wrapper events",
    });
    backfillRule.addTarget(new targets.LambdaFunction(backfillLambda));

    // ─────────────────────────────────────────────
    // LAMBDA: CUR reconciliation (daily)
    // ─────────────────────────────────────────────

    const reconcileLambda = new lambda.Function(this, "ReconcileLambda", {
      ...lambdaDefaults,
      functionName: "bedrock-cost-reconcile",
      code: lambda.Code.fromAsset("../lambdas/reconcile"),
      handler: "handler.lambda_handler",
      timeout: cdk.Duration.minutes(15),
      memorySize: 1024, // CUR processing is memory-intensive
    });
    eventsTable.grantReadData(reconcileLambda);
    reconciliationTable.grantReadWriteData(reconcileLambda);
    alertTopic.grantPublish(reconcileLambda);

    if (props.curBucketName) {
      const curBucket = s3.Bucket.fromBucketName(
        this,
        "CurBucket",
        props.curBucketName
      );
      curBucket.grantRead(reconcileLambda);
    }

    // Runs at 06:00 UTC daily (after CUR is typically available)
    const reconcileRule = new events.Rule(this, "ReconcileSchedule", {
      schedule: events.Schedule.cron({ hour: "6", minute: "0" }),
      description: "Daily CUR vs computed cost reconciliation",
    });
    reconcileRule.addTarget(new targets.LambdaFunction(reconcileLambda));

    // ─────────────────────────────────────────────
    // LAMBDA: query API (dashboard read layer)
    // ─────────────────────────────────────────────

    const queryLambda = new lambda.Function(this, "QueryLambda", {
      ...lambdaDefaults,
      functionName: "bedrock-cost-query",
      code: lambda.Code.fromAsset("../lambdas/query_api"),
      handler: "handler.lambda_handler",
      memorySize: 512,
    });
    eventsTable.grantReadData(queryLambda);
    priceTable.grantReadData(queryLambda);
    reconciliationTable.grantReadData(queryLambda);

    // ─────────────────────────────────────────────
    // API GATEWAY
    // ─────────────────────────────────────────────

    const api = new apigateway.RestApi(this, "BedrockCostApi", {
      restApiName: "bedrock-cost-explorer",
      description: "Bedrock Cost Explorer — ingest + query API",
      deployOptions: {
        stageName: "v1",
        throttlingBurstLimit: 500,
        throttlingRateLimit: 100,
        metricsEnabled: true,
        loggingLevel: apigateway.MethodLoggingLevel.ERROR,
      },
      defaultCorsPreflightOptions: {
        allowOrigins: apigateway.Cors.ALL_ORIGINS, // lock down in prod
        allowMethods: apigateway.Cors.ALL_METHODS,
        allowHeaders: ["Content-Type", "X-Api-Key"],
      },
    });

    // API key for SDK wrapper authentication
    const apiKey = api.addApiKey("SdkWrapperApiKey", {
      apiKeyName: "bedrock-sdk-wrapper",
      description: "Used by instrumentation SDK wrappers",
    });
    const usagePlan = api.addUsagePlan("DefaultUsagePlan", {
      name: "default",
      throttle: { rateLimit: 100, burstLimit: 500 },
    });
    usagePlan.addApiKey(apiKey);
    usagePlan.addApiStage({ stage: api.deploymentStage });

    const ingestIntegration = new apigateway.LambdaIntegration(ingestLambda, {
      requestTemplates: { "application/json": '{ "statusCode": "200" }' },
    });
    const queryIntegration = new apigateway.LambdaIntegration(queryLambda);

    // POST /events  — SDK wrapper ingestion (requires API key)
    const eventsResource = api.root.addResource("events");
    eventsResource.addMethod("POST", ingestIntegration, {
      apiKeyRequired: true,
    });

    // GET /query/*  — dashboard read endpoints (internal; add Cognito in prod)
    const queryResource = api.root.addResource("query");
    const queryProxy = queryResource.addResource("{proxy+}");
    queryProxy.addMethod("GET", queryIntegration);

    // ─────────────────────────────────────────────
    // OUTPUTS
    // ─────────────────────────────────────────────

    this.eventsTableName = eventsTable.tableName;
    this.priceTableName = priceTable.tableName;
    this.ingestApiUrl = api.url;

    new cdk.CfnOutput(this, "IngestApiUrl", {
      value: api.url + "events",
      description: "Endpoint for SDK wrapper to POST events to",
    });
    new cdk.CfnOutput(this, "QueryApiUrl", {
      value: api.url + "query",
      description: "Base URL for dashboard query endpoints",
    });
    new cdk.CfnOutput(this, "ApiKeyId", {
      value: apiKey.keyId,
      description: "API key ID — retrieve value from console or CLI",
    });
    new cdk.CfnOutput(this, "AlertTopicArn", {
      value: alertTopic.topicArn,
    });
  }
}
