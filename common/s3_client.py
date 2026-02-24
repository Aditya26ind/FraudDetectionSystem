from __future__ import annotations
import boto3
from botocore.client import Config
from common.config import get_settings

_settings = get_settings()


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=_settings.s3_endpoint,
        aws_access_key_id=_settings.s3_access_key,
        aws_secret_access_key=_settings.s3_secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket_exists(client=None):
    client = client or get_s3_client()
    try:
        client.head_bucket(Bucket=_settings.s3_bucket)
    except Exception:
        client.create_bucket(Bucket=_settings.s3_bucket)
