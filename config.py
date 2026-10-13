from dotenv import load_dotenv
import os

load_dotenv()

mongo_url = os.getenv("mongo_url")
webhook_post = os.getenv("webhook_post")
webhook_url = os.getenv("webhook_url")
vb_webhook_url = os.getenv("vb_webhook_url")
database_name = os.getenv("database_name")
# Tenant frontend base URL — used to build the /r/{token} link put in the SMS and printed
# to the terminal.
FEEDBACK_LINK_BASE_URL = os.getenv("FEEDBACK_LINK_BASE_URL", "")

# --- Optional DigitalOcean Spaces (S3-compatible) recording upload ---
IS_S3_UPLOAD_ENABLED = os.getenv("IS_S3_UPLOAD_ENABLED", "false").lower() == "true"

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "")
S3_REGION = os.getenv("S3_REGION", "")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "ringingo-recording")
S3_KEY_PREFIX = os.getenv("S3_KEY_PREFIX", "ringingo-recording")
S3_PUBLIC_BASE_URL = os.getenv("S3_PUBLIC_BASE_URL", "")
DELETE_LOCAL_AFTER_UPLOAD = os.getenv("DELETE_LOCAL_AFTER_UPLOAD", "true").lower() == "true"
