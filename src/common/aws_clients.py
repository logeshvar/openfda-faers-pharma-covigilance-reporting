from __future__ import annotations

from typing import Any

import boto3

from src.common.config import S3Settings


def build_s3_client(s3_settings: S3Settings):
    client_kwargs: dict[str, Any] = {
        "service_name": "s3",
        "region_name": s3_settings.region_name,
    }
    if s3_settings.endpoint_url:
        client_kwargs["endpoint_url"] = s3_settings.endpoint_url
    if s3_settings.access_key_id:
        client_kwargs["aws_access_key_id"] = s3_settings.access_key_id
    if s3_settings.secret_access_key:
        client_kwargs["aws_secret_access_key"] = s3_settings.secret_access_key
    return boto3.client(**client_kwargs)
