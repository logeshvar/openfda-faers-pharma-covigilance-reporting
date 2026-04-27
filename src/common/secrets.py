from __future__ import annotations

import json
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError


def load_json_secret(secret_id: str, region_name: str) -> dict[str, Any]:
    client = boto3.client("secretsmanager", region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_id)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "Unknown")
        raise RuntimeError(
            f"Unable to read Secrets Manager secret {secret_id!r} in region {region_name!r}; "
            f"AWS returned {error_code}. Check the AWS credentials mounted into Airflow and "
            "confirm they can call secretsmanager:GetSecretValue for this secret."
        ) from exc
    except BotoCoreError as exc:
        raise RuntimeError(
            f"Unable to read Secrets Manager secret {secret_id!r} in region {region_name!r}: {exc}"
        ) from exc
    secret_string = response.get("SecretString")

    if not secret_string:
        raise ValueError(f"Secrets Manager secret {secret_id!r} does not contain a SecretString value.")

    payload = json.loads(secret_string)
    if not isinstance(payload, dict):
        raise ValueError(f"Secrets Manager secret {secret_id!r} must be a JSON object.")

    return payload
