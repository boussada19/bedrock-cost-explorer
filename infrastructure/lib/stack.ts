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
import * as cognito from "aws-cdk-lib/aws-cognito";
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import { Construct } from "constructs";

export interface BedrockCostExplorerProps extends cdk.StackProps {
  alertEmail: string;
  eventRetentionDays?: number;
  curBucketName?: string;
  costAllocationTags?: Record<string, string>;
}

export class BedrockCostExplorerStack extends cdk.Stack {
  public readonly eventsTableName: string;
  public readonly priceTableName: string;
  public readonly ingestApiUrl: string;

  constructor(scope: Construct, id: string, props: BedrockCostExplorerProps) {
    super(scope, id, props);

    const eventRetentionDays = props.eventRetentionDays ?? 90;

    // ── DynamoDB tables ────────────────────────────────────────────

    const eventsTable = new dynamodb.Table(this, "BedrockEventsTable", {
      tableName: "bedrock_events",
      partitionKey: { name: "PK", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "SK", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      timeToLiveAttribute: "ttl",
      pointInTimeRecoverySpecification: { pointInTimeRecoveryEnabled: true },
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      stream: dynamodb.StreamViewType.NEW_IMAGE,
    });

    for (const gsi of [
      { name: "gsi_agent_time",   pk: "agent_id" },
      { name: "gsi_user_time",    pk: "user_id" },
      { name: "gsi_app_time",     pk: "application_id" },
      { name: "gsi_model_time",   pk: "model_id" },
      { name: "gsi_account_time", pk: "account_id" },
    ]) {
      eventsTable.addGlobalSecondaryIndex({
        indexName: gsi.name,
        partitionKey: { name: gsi.pk, type: dynamodb.AttributeType.STRING },
        sortKey: { name: "timestamp", type: dynamodb.AttributeType.STRING },
        projectionType: dynamodb.ProjectionType.ALL,
      });
    }

    // MSP tenant index — partition key is `source` (the client/tenant ID).
    // Enables O(1) per-tenant queries without full-table scans.
    // `source` values: 'wrapper' | 'cloudwatch_backfill' | '<client-id>'
    eventsTable.addGlobalSecondaryIndex({
      indexName:      "SourceTimestampIndex",
      partitionKey:   { name: "source",    type: dynamodb.AttributeType.STRING },
      sortKey:        { name: "timestamp", type: dynamodb.AttributeType.STRING },
      projectionType: dynamodb.ProjectionType.ALL,
    });

    const priceTable = new dynamodb.Table(this, "BedrockPriceTable", {
      tableName: "bedrock_price_table",
      partitionKey: { name: "PK", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "SK", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });
    priceTable.addGlobalSecondaryIndex({
      indexName: "gsi_active_prices",
      partitionKey: { name: "effective_until", type: dynamodb.AttributeType.STRING },
      sortKey:      { name: "model_id",        type: dynamodb.AttributeType.STRING },
      projectionType: dynamodb.ProjectionType.ALL,
    });

    const reconciliationTable = new dynamodb.Table(this, "BedrockReconciliationTable", {
      tableName: "bedrock_reconciliation_runs",
      partitionKey: { name: "PK", type: dynamodb.AttributeType.STRING },
      sortKey:      { name: "SK", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    // ── SQS ───────────────────────────────────────────────────────

    const costEnrichmentDlq = new sqs.Queue(this, "CostEnrichmentDlq", {
      queueName: "bedrock-cost-enrichment-dlq",
      retentionPeriod: cdk.Duration.days(14),
    });

    const costEnrichmentQueue = new sqs.Queue(this, "CostEnrichmentQueue", {
      queueName: "bedrock-cost-enrichment",
      visibilityTimeout: cdk.Duration.seconds(30),
      deadLetterQueue: { queue: costEnrichmentDlq, maxReceiveCount: 3 },
    });

    // ── SNS ───────────────────────────────────────────────────────

    const alertTopic = new sns.Topic(this, "BedrockCostAlerts", {
      topicName: "bedrock-cost-alerts",
      displayName: "Bedrock Cost Explorer Alerts",
    });
    if (props.alertEmail) {
      alertTopic.addSubscription(new subscriptions.EmailSubscription(props.alertEmail));
    }

    // ── Shared Lambda config ──────────────────────────────────────
    //
    // Using a helper function rather than a Partial<> spread so TypeScript
    // knows `runtime` is always Runtime (not Runtime | undefined).

    const commonEnv: Record<string, string> = {
      EVENTS_TABLE:              eventsTable.tableName,
      PRICE_TABLE:               priceTable.tableName,
      RECONCILIATION_TABLE:      reconciliationTable.tableName,
      COST_ENRICHMENT_QUEUE_URL: costEnrichmentQueue.queueUrl,
      ALERT_TOPIC_ARN:           alertTopic.topicArn,
      EVENT_RETENTION_DAYS:      String(eventRetentionDays),
      POWERTOOLS_SERVICE_NAME:   "bedrock-cost-explorer",
      LOG_LEVEL:                 "INFO",
      // MSP: name of the GSI that partitions events by tenant (source field)
      TENANT_INDEX:              "SourceTimestampIndex",
    };
    if (props.curBucketName) {
      commonEnv["CUR_BUCKET"] = props.curBucketName;
    }

    /** Returns fully-typed FunctionProps with all required fields present. */
    const fn = (overrides: Omit<lambda.FunctionProps, "runtime" | "environment" | "logRetention">): lambda.FunctionProps => ({
      runtime:      lambda.Runtime.PYTHON_3_12,
      environment:  commonEnv,
      logRetention: logs.RetentionDays.ONE_MONTH,
      memorySize:   256,
      timeout:      cdk.Duration.seconds(30),
      ...overrides,
    });

    // ── Lambda: ingest ────────────────────────────────────────────

    const ingestLambda = new lambda.Function(this, "IngestLambda", fn({
      functionName: "bedrock-cost-ingest",
      code:         lambda.Code.fromAsset("../lambdas/ingest"),
      handler:      "handler.lambda_handler",
      memorySize:   128,
      timeout:      cdk.Duration.seconds(10),
    }));
    eventsTable.grantWriteData(ingestLambda);
    costEnrichmentQueue.grantSendMessages(ingestLambda);

    // ── Lambda: cost compute ──────────────────────────────────────

    const costComputeLambda = new lambda.Function(this, "CostComputeLambda", fn({
      functionName:                "bedrock-cost-compute",
      code:                        lambda.Code.fromAsset("../lambdas/cost_compute"),
      handler:                     "handler.lambda_handler",
      reservedConcurrentExecutions: 10,
    }));
    eventsTable.grantReadWriteData(costComputeLambda);
    priceTable.grantReadData(costComputeLambda);
    costComputeLambda.addEventSource(new SqsEventSource(costEnrichmentQueue, {
      batchSize:          10,
      maxBatchingWindow:  cdk.Duration.seconds(5),
    }));

    // ── Lambda: backfill ──────────────────────────────────────────

    const backfillLambda = new lambda.Function(this, "BackfillLambda", fn({
      functionName: "bedrock-cost-backfill",
      code:         lambda.Code.fromAsset("../lambdas/backfill"),
      handler:      "handler.lambda_handler",
      timeout:      cdk.Duration.minutes(5),
      memorySize:   512,
    }));
    eventsTable.grantReadWriteData(backfillLambda);
    priceTable.grantReadData(backfillLambda);
    costEnrichmentQueue.grantSendMessages(backfillLambda);
    backfillLambda.addToRolePolicy(new iam.PolicyStatement({
      effect:    iam.Effect.ALLOW,
      actions:   ["logs:FilterLogEvents", "logs:DescribeLogGroups", "logs:DescribeLogStreams"],
      resources: ["*"],
    }));
    backfillLambda.addToRolePolicy(new iam.PolicyStatement({
      effect:    iam.Effect.ALLOW,
      actions:   ["bedrock:GetModelInvocationLoggingConfiguration"],
      resources: ["*"],
    }));

    const backfillRule = new events.Rule(this, "BackfillSchedule", {
      schedule:    events.Schedule.rate(cdk.Duration.hours(1)),
      description: "Trigger backfill Lambda to reconcile CloudWatch vs wrapper events",
    });
    backfillRule.addTarget(new targets.LambdaFunction(backfillLambda));

    // ── Lambda: reconcile ─────────────────────────────────────────

    const reconcileLambda = new lambda.Function(this, "ReconcileLambda", fn({
      functionName: "bedrock-cost-reconcile",
      code:         lambda.Code.fromAsset("../lambdas/reconcile"),
      handler:      "handler.lambda_handler",
      timeout:      cdk.Duration.minutes(15),
      memorySize:   1024,
    }));
    eventsTable.grantReadData(reconcileLambda);
    reconciliationTable.grantReadWriteData(reconcileLambda);
    alertTopic.grantPublish(reconcileLambda);

    if (props.curBucketName) {
      s3.Bucket.fromBucketName(this, "CurBucket", props.curBucketName)
               .grantRead(reconcileLambda);
    }

    const reconcileRule = new events.Rule(this, "ReconcileSchedule", {
      schedule:    events.Schedule.cron({ hour: "6", minute: "0" }),
      description: "Daily CUR vs computed cost reconciliation",
    });
    reconcileRule.addTarget(new targets.LambdaFunction(reconcileLambda));

    // ── Lambda: query API ─────────────────────────────────────────

    const queryLambda = new lambda.Function(this, "QueryLambda", fn({
      functionName: "bedrock-cost-query",
      code:         lambda.Code.fromAsset("../lambdas/query_api"),
      handler:      "handler.lambda_handler",
      memorySize:   512,
    }));
    eventsTable.grantReadData(queryLambda);
    priceTable.grantReadData(queryLambda);
    reconciliationTable.grantReadData(queryLambda);

    // ── Cognito User Pool (MSP authentication) ────────────────────
    //
    // Each user carries a custom:tenant_id attribute:
    //   - a client user → their tenant ID, e.g. "client-alpha"
    //   - an Atomic Computing admin → "*" (can view all tenants)
    //
    // The query Lambda reads tenant scope from the VERIFIED JWT claims,
    // never from a client-supplied query param — so a logged-in client
    // physically cannot read another client's data.

    const userPool = new cognito.UserPool(this, "EnterpriseHubUserPool", {
      userPoolName: "atomic-computing-hub",
      selfSignUpEnabled: false,                 // admins create client accounts
      signInAliases: { email: true },
      autoVerify: { email: true },
      standardAttributes: {
        email: { required: true, mutable: false },
      },
      customAttributes: {
        // "*" = admin (all tenants); otherwise the client's tenant id
        tenant_id: new cognito.StringAttribute({ minLen: 1, maxLen: 64, mutable: true }),
        // "admin" | "client"
        role:      new cognito.StringAttribute({ minLen: 1, maxLen: 16, mutable: true }),
      },
      passwordPolicy: {
        minLength: 12,
        requireLowercase: true,
        requireUppercase: true,
        requireDigits: true,
        requireSymbols: true,
      },
      accountRecovery: cognito.AccountRecovery.EMAIL_ONLY,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    const userPoolClient = userPool.addClient("HubWebClient", {
      userPoolClientName: "hub-web",
      authFlows: {
        userSrp: true,
        userPassword: true,   // allows the simple USER_PASSWORD_AUTH login flow
      },
      accessTokenValidity:  cdk.Duration.hours(8),
      idTokenValidity:      cdk.Duration.hours(8),
      refreshTokenValidity: cdk.Duration.days(30),
      preventUserExistenceErrors: true,
    });

    // Pass the user-pool id into the query Lambda env so it can be referenced
    queryLambda.addEnvironment("USER_POOL_ID", userPool.userPoolId);

    // ── API Gateway ───────────────────────────────────────────────

    const api = new apigateway.RestApi(this, "BedrockCostApi", {
      restApiName: "bedrock-cost-explorer",
      description: "Bedrock Cost Explorer — ingest + query API",
      deployOptions: {
        stageName:            "v1",
        throttlingBurstLimit: 500,
        throttlingRateLimit:  100,
        metricsEnabled:       false,
        // loggingLevel omitted — requires a CloudWatch Logs role ARN configured
        // at the account level in API Gateway → Settings before it can be enabled.
      },
      defaultCorsPreflightOptions: {
        allowOrigins: apigateway.Cors.ALL_ORIGINS,
        allowMethods: apigateway.Cors.ALL_METHODS,
        allowHeaders: ["Content-Type", "X-Api-Key", "Authorization"],
      },
    });

    const apiKey = api.addApiKey("SdkWrapperApiKey", {
      apiKeyName:  "bedrock-sdk-wrapper",
      description: "Used by instrumentation SDK wrappers — v2",
    });
    const usagePlan = api.addUsagePlan("DefaultUsagePlan", {
      name:     "default",
      throttle: { rateLimit: 100, burstLimit: 500 },
    });
    usagePlan.addApiKey(apiKey);
    usagePlan.addApiStage({ stage: api.deploymentStage });

    const eventsResource = api.root.addResource("events");
    eventsResource.addMethod("POST",
      new apigateway.LambdaIntegration(ingestLambda, {
        requestTemplates: { "application/json": '{ "statusCode": "200" }' },
      }),
      { apiKeyRequired: true }
    );

    // Cognito authorizer — every /query/* request must carry a valid
    // ID token in the Authorization header. The Lambda then derives the
    // tenant from the token's custom:tenant_id claim.
    const cognitoAuthorizer = new apigateway.CognitoUserPoolsAuthorizer(
      this, "HubAuthorizer", {
        cognitoUserPools: [userPool],
        authorizerName: "atomic-hub-authorizer",
        identitySource: "method.request.header.Authorization",
      },
    );

    const queryProxy = api.root.addResource("query").addResource("{proxy+}");
    queryProxy.addMethod("GET",
      new apigateway.LambdaIntegration(queryLambda),
      {
        authorizer: cognitoAuthorizer,
        authorizationType: apigateway.AuthorizationType.COGNITO,
      },
    );

    // ── Outputs ───────────────────────────────────────────────────

    this.eventsTableName = eventsTable.tableName;
    this.priceTableName  = priceTable.tableName;
    this.ingestApiUrl    = api.url;

    new cdk.CfnOutput(this, "IngestApiUrl", {
      value:       api.url + "events",
      description: "Endpoint for SDK wrapper to POST events to",
    });
    new cdk.CfnOutput(this, "QueryApiUrl", {
      value:       api.url + "query",
      description: "Base URL for dashboard query endpoints",
    });
    new cdk.CfnOutput(this, "ApiKeyId", {
      value:       apiKey.keyId,
      description: "API key ID — retrieve value from console or CLI",
    });
    new cdk.CfnOutput(this, "AlertTopicArn", { value: alertTopic.topicArn });

    // ── Cognito outputs (needed by the dashboard login page) ───────
    new cdk.CfnOutput(this, "UserPoolId", {
      value:       userPool.userPoolId,
      description: "Cognito User Pool ID — used by dashboard login",
    });
    new cdk.CfnOutput(this, "UserPoolClientId", {
      value:       userPoolClient.userPoolClientId,
      description: "Cognito App Client ID — used by dashboard login",
    });
    new cdk.CfnOutput(this, "CognitoRegion", {
      value:       this.region,
      description: "Region for Cognito SDK calls",
    });
  }
}