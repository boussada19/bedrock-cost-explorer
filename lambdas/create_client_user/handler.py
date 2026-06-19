"""
createClientUser Lambda — Atomic Computing admin tool.

Creates a new Cognito user, assigns them to the 'Clients' group,
and stamps their profile with the tenant_id matching their 'source' tag.

This Lambda is NEVER exposed via API Gateway. Invoke directly:

  aws lambda invoke \
    --function-name bedrock-create-client-user \
    --payload '{"email":"contact@acme.com","tenant_id":"acme-corp","temp_password":"Welcome1!@#"}' \
    --region eu-central-1 \
    response.json

Payload fields:
  email         (required) — the client's login email
  tenant_id     (required) — must match the 'source' field in their events
                             e.g. 'client-alpha', 'acme-corp'
  temp_password (optional) — if omitted Cognito auto-generates one and emails it
  given_name    (optional) — display name
  company_name  (optional) — stored as a note in the user profile
"""

import json
import logging
import os
import re
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel("INFO")

cognito = boto3.client("cognito-idp")
USER_POOL_ID = os.environ["USER_POOL_ID"]

TENANT_ID_RE = re.compile(r'^[a-zA-Z0-9_-]{1,64}$')


def lambda_handler(event, context):
    # Support both direct invocation and API Gateway proxy format
    if "body" in event:
        try:
            payload = json.loads(event["body"] or "{}")
        except json.JSONDecodeError:
            return _resp(400, {"error": "Invalid JSON body"})
    else:
        payload = event

    email       = (payload.get("email") or "").strip().lower()
    tenant_id   = (payload.get("tenant_id") or "").strip()
    temp_pw     = payload.get("temp_password")      # optional
    given_name  = payload.get("given_name", "")
    company     = payload.get("company_name", "")

    # ── Validate ──────────────────────────────────────────────────
    errors = []
    if not email or "@" not in email:
        errors.append("'email' is required and must be a valid email address.")
    if not tenant_id or not TENANT_ID_RE.match(tenant_id):
        errors.append(
            "'tenant_id' is required and must contain only letters, digits, "
            "hyphens, or underscores (1-64 chars)."
        )
    if errors:
        return _resp(400, {"error": "Validation failed", "details": errors})

    # ── Check for duplicate ───────────────────────────────────────
    try:
        cognito.admin_get_user(UserPoolId=USER_POOL_ID, Username=email)
        return _resp(409, {
            "error": "User already exists",
            "email": email,
        })
    except ClientError as e:
        if e.response["Error"]["Code"] != "UserNotFoundException":
            logger.exception("Unexpected error checking existing user")
            return _resp(500, {"error": str(e)})

    # ── Create user ───────────────────────────────────────────────
    user_attrs = [
        {"Name": "email",              "Value": email},
        {"Name": "email_verified",     "Value": "true"},
        {"Name": "custom:tenant_id",   "Value": tenant_id},
        {"Name": "custom:role",        "Value": "client"},
    ]
    if given_name:
        user_attrs.append({"Name": "given_name", "Value": given_name})

    create_kwargs = {
        "UserPoolId":          USER_POOL_ID,
        "Username":            email,
        "UserAttributes":      user_attrs,
        "DesiredDeliveryMediums": ["EMAIL"],
        "ForceAliasCreation":  False,
    }
    if temp_pw:
        create_kwargs["TemporaryPassword"] = temp_pw
        create_kwargs["MessageAction"] = "SUPPRESS"  # don't send Cognito's default email

    try:
        resp = cognito.admin_create_user(**create_kwargs)
        username = resp["User"]["Username"]
    except ClientError as e:
        logger.exception("Failed to create Cognito user")
        return _resp(500, {"error": str(e)})

    # ── Add to Clients group ──────────────────────────────────────
    try:
        cognito.admin_add_user_to_group(
            UserPoolId=USER_POOL_ID,
            Username=username,
            GroupName="Clients",
        )
    except ClientError as e:
        logger.error("User created but failed to add to Clients group: %s", e)
        # Don't roll back — user exists, group membership can be added manually

    logger.info(
        "Created client user: email=%s tenant_id=%s username=%s",
        email, tenant_id, username,
    )

    return _resp(201, {
        "status":    "created",
        "username":  username,
        "email":     email,
        "tenant_id": tenant_id,
        "group":     "Clients",
        "note": (
            f"User will receive a temporary password via email. "
            f"They must change it on first login. "
            f"Their dashboard will be locked to source='{tenant_id}'."
        ),
    })


def _resp(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, indent=2),
    }
