import os
import logging
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "eu-central-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_BASE_FOLDER = os.getenv("S3_BASE_FOLDER", "facebook-scraper-content")


def _get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )


def upload_file_to_s3(local_path: str, s3_key: str, content_type: str = None):
    """
    Upload a local file to S3 with public-read ACL.
    Returns (public_url, full_s3_key).
    """
    full_key = f"{S3_BASE_FOLDER}/{s3_key}"
    client = _get_s3_client()

    extra_args = {"ACL": "public-read"}
    if content_type:
        extra_args["ContentType"] = content_type

    try:
        client.upload_file(local_path, S3_BUCKET_NAME, full_key, ExtraArgs=extra_args)
        public_url = f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{full_key}"
        logging.info(f"[S3] Uploaded {local_path} → {public_url}")
        return public_url, full_key
    except ClientError as exc:
        logging.error(f"[S3] Upload failed for {local_path}: {exc}")
        raise


def generate_presigned_url(s3_key: str, expiry: int = 604800) -> str:
    """Generate a presigned URL for an existing S3 key (fallback if ACL public fails)."""
    client = _get_s3_client()
    try:
        return client.generate_presigned_url(
            "get_object",
            Params={"Bucket": S3_BUCKET_NAME, "Key": s3_key},
            ExpiresIn=expiry,
        )
    except ClientError as exc:
        logging.error(f"[S3] Presign failed for {s3_key}: {exc}")
        return ""


def delete_local_file(path: str):
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception as exc:
        logging.warning(f"[S3] Could not delete local file {path}: {exc}")
