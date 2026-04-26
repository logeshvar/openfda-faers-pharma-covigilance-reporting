from __future__ import annotations

import json
from typing import Any

import boto3


def load_json_secret(secret_id: str, region_name: str) -> dict[str, Any]:
    client = boto3.client("secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=secret_id)
    secret_string = response.get("SecretString")

    if not secret_string:
        raise ValueError(f"Secrets Manager secret {secret_id!r} does not contain a SecretString value.")

    payload = json.loads(secret_string)
    if not isinstance(payload, dict):
        raise ValueError(f"Secrets Manager secret {secret_id!r} must be a JSON object.")

    return payload
