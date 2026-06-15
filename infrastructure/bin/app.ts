#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "aws-cdk-lib";
import { BedrockCostExplorerStack } from "./lib/stack";

const app = new cdk.App();

new BedrockCostExplorerStack(app, "BedrockCostExplorer", {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION ?? "us-east-1",
  },
  alertEmail: process.env.ALERT_EMAIL ?? "platform-alerts@example.com",
  curBucketName: process.env.CUR_BUCKET_NAME, // optional: set when CUR is configured
  eventRetentionDays: 90,
  tags: {
    Project: "BedrockCostExplorer",
    ManagedBy: "CDK",
  },
});
