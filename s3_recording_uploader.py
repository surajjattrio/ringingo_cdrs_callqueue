import os
import mimetypes
from config import (
    IS_S3_UPLOAD_ENABLED, S3_ENDPOINT_URL, S3_REGION, S3_ACCESS_KEY, S3_SECRET_KEY,
    S3_BUCKET_NAME, S3_KEY_PREFIX, S3_PUBLIC_BASE_URL, DELETE_LOCAL_AFTER_UPLOAD
)

s3_client = None
if IS_S3_UPLOAD_ENABLED:
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError

    s3_client = boto3.client(
        's3',
        region_name=S3_REGION,
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(connect_timeout=5, read_timeout=10, retries={'max_attempts': 2}),
    )

def upload_recording_to_s3(local_path, tenant_uuid, recording_uuid):
    if not IS_S3_UPLOAD_ENABLED:
        return local_path, "local"

    if not os.path.exists(local_path):
        print(f"[S3 upload] Local recording not found, keeping local reference: {local_path}")
        return local_path, "local"

    # local recordings layout: {domain}/{date}/{filename}.wav
    # e.g. /usr/local/freeswitch/recordings/mt.ringingo.com/2026-07-23/<uuid>.wav
    #   -> {S3_KEY_PREFIX}/mt.ringingo.com/2026-07-23/<uuid>.wav
    path_parts = local_path.replace("\\", "/").rstrip("/").split("/")
    filename = path_parts[-1] if path_parts else f"{recording_uuid}.wav"
    date_part = path_parts[-2] if len(path_parts) >= 2 else ""
    domain_part = path_parts[-3] if len(path_parts) >= 3 else tenant_uuid
    object_key = f"{S3_KEY_PREFIX}/{domain_part}/{date_part}/{filename}"

    content_type, _ = mimetypes.guess_type(filename)
    if not content_type:
        content_type = "audio/wav"

    print(f"[S3 upload] Starting upload of {local_path} to s3://{S3_BUCKET_NAME}/{object_key}")
    try:
        s3_client.upload_file(
            local_path, S3_BUCKET_NAME, object_key,
            ExtraArgs={
                'ACL': 'public-read',
                'ContentType': content_type,
                'ContentDisposition': 'inline',
            }
        )
        public_url = f"{S3_PUBLIC_BASE_URL}/{object_key}"
        print(f"[S3 upload] Success: {public_url}")

        if DELETE_LOCAL_AFTER_UPLOAD:
            try:
                os.remove(local_path)
            except Exception as delete_error:
                print(f"[S3 upload] Uploaded OK but failed to delete local file {local_path}: {delete_error}")

        return public_url, "uploaded"
    except ClientError as e:
        print(f"[S3 upload] FAILED (client/auth/bucket error) for {object_key}: {e}")
        return local_path, "failed"
    except Exception as e:
        print(f"[S3 upload] FAILED (unexpected error, keeping local file for retry) for {object_key}: {e}")
        return local_path, "failed"
