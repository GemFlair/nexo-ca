import os
from pathlib import Path
from typing import Union
from dotenv import load_dotenv

# ===========================================================
# 🌍 Environment Auto-Detection Loader
# ===========================================================

# Determine which environment is active (default = local_dev)
ENVIRONMENT = (os.getenv("ENVIRONMENT", "local_dev") or "local_dev").lower()

# Map environment label to .env file
ENV_FILE_MAP = {
    "local_dev": ".env.local_dev",
    "dev": ".env.dev",
    "prod": ".env.prod",
}

# Pick the correct .env file for the active environment
env_filename = ENV_FILE_MAP.get(ENVIRONMENT, ".env.local_dev")

# Build absolute path to the .env file (repo root)
repo_root = Path(__file__).resolve().parents[2]
env_path = repo_root / env_filename

# Load the selected .env file, if it exists
if env_path.exists():
    load_dotenv(dotenv_path=env_path, override=True)
    print(f"✅ Loaded environment: {ENVIRONMENT} ({env_filename})")
else:
    print(f"⚠️ Warning: {env_filename} not found — using system environment variables only.")

# ===========================================================
# 🪄 Environment-Aware Helpers
# ===========================================================

BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "nexo-storage-ca")

if ENVIRONMENT == "local_dev":
    S3_BASE_PREFIX = None
else:
    default_prefix = {"dev": "dev", "prod": "prod"}.get(ENVIRONMENT)
    S3_BASE_PREFIX = os.getenv("S3_BASE_PREFIX", default_prefix or "dev")


def build_s3_path(subpath: str) -> str:
    """Return a full S3 path like s3://bucket/dev/input_data/pdf"""
    if S3_BASE_PREFIX is None:
        raise RuntimeError("S3 access disabled in local_dev environment")
    return f"s3://{BUCKET_NAME}/{S3_BASE_PREFIX}/{subpath.lstrip('/')}"


def build_local_path(subpath: str) -> str:
    """Return a local absolute path within the repo."""
    return str(repo_root / subpath)


def get_environment() -> str:
    """Return the current environment (local_dev / dev / prod)."""
    return ENVIRONMENT


def get_bucket_prefix() -> str:
    """Return full S3 prefix for the active environment."""
    if not S3_BASE_PREFIX:
        return ""
    return f"{BUCKET_NAME}/{S3_BASE_PREFIX}"


def get(key: str, default=None) -> Union[str, None]:
    """Get an environment variable value."""
    return os.getenv(key, default)

# ===========================================================
# 🌐 Unified Public Media Base URL
# ===========================================================

def get_public_media_base() -> str:
    """
    Return the base public URL for serving processed media assets (logos, banners, etc.)
    based entirely on environment configuration.
    Priority:
      1. PUBLIC_MEDIA_BASE from .env (recommended)
      2. CDN_BASE_URL + S3_BASE_PREFIX (for dev/prod)
      3. http://127.0.0.1:8000 (local fallback)
      4. https://{bucket}.s3.amazonaws.com/{prefix} (final fallback)
    """
    env = get_environment().lower()

    # 1️⃣ Explicit from .env
    explicit = os.getenv("PUBLIC_MEDIA_BASE", "").rstrip("/")
    if explicit:
        return explicit

    # 2️⃣ Derive from CDN + prefix
    cdn = os.getenv("CDN_BASE_URL", "").rstrip("/")
    prefix = os.getenv("S3_BASE_PREFIX", env).strip("/")
    bucket = os.getenv("S3_BUCKET_NAME", "nexo-storage-ca")

    if cdn and prefix:
        return f"{cdn}/{prefix}".rstrip("/")

    # 3️⃣ Local fallback
    if env == "local_dev":
        return "http://127.0.0.1:8000"

    # 4️⃣ S3 fallback
    return f"https://{bucket}.s3.amazonaws.com/{prefix}".rstrip("/")


def get_static_base_url() -> str:
    """Deprecated alias for backward compatibility."""
    return get_public_media_base()

# ===========================================================
# 📁 Path Helpers — PDF Processor
# ===========================================================

def get_input_pdf_dir() -> str:
    if ENVIRONMENT == "local_dev":
        return build_local_path(get("LOCAL_INPUT_PDF_DIR", "backend/input_data/pdf"))
    return get("S3_INPUT_PDF_PATH") or build_s3_path("input_data/pdf")


def get_output_announcements_dir() -> str:
    if ENVIRONMENT == "local_dev":
        return build_local_path(get("LOCAL_ANNOUNCEMENTS_DIR", "backend/output_data/announcements"))
    return get("S3_OUTPUT_ANNOUNCEMENTS_PATH") or build_s3_path("output_data/announcements")


def get_processed_pdf_dir() -> str:
    if ENVIRONMENT == "local_dev":
        return build_local_path(get("LOCAL_PROCESSED_PDF_DIR", "backend/output_data/processed_pdf"))
    return get("S3_PROCESSED_PDF_PATH") or build_s3_path("output_data/processed_pdf")


def get_error_dir() -> str:
    return build_local_path(get("LOCAL_ERROR_DIR", "error_reports"))


def get_temp_dir() -> str:
    return build_local_path(get("LOCAL_TEMP_DIR", "tmp"))


def get_image_temp_dir() -> str:
    return build_local_path(get("LOCAL_IMAGE_TEMP_DIR", "input_data/images/tmp"))


def get_default_logo_path() -> str:
    return build_local_path(get("LOCAL_DEFAULT_LOGO_PATH", "input_data/images/defaults/Default_logo.png"))


def get_default_banner_path() -> str:
    return build_local_path(get("LOCAL_DEFAULT_BANNER_PATH", "input_data/images/defaults/Default_banner.webp"))

# ===========================================================
# 📁 Path Helpers — CSV Processor
# ===========================================================

def get_raw_csv_dir() -> str:
    if ENVIRONMENT == "local_dev":
        return build_local_path(get("LOCAL_RAW_CSV_DIR", "backend/input_data/csv/eod_csv"))
    return get("S3_RAW_CSV_PATH") or build_s3_path("input_data/csv/eod_csv")


def get_processed_csv_dir() -> str:
    if ENVIRONMENT == "local_dev":
        return build_local_path(get("LOCAL_PROCESSED_CSV_DIR", "backend/output_data/processed_csv"))
    return get("S3_PROCESSED_CSV_PATH") or build_s3_path("output_data/processed_csv")


def get_static_csv_dir() -> str:
    if ENVIRONMENT == "local_dev":
        return build_local_path(get("LOCAL_STATIC_DIR", "backend/input_data/csv/static"))
    return get("S3_STATIC_CSV_PATH") or build_s3_path("input_data/csv/static")

# ===========================================================
# 📁 Path Helpers — Image Processor
# ===========================================================

def get_raw_images_dir() -> str:
    if ENVIRONMENT == "local_dev":
        return build_local_path(get("LOCAL_RAW_IMAGE_DIR", "backend/input_data/images/raw_images"))
    return get("S3_RAW_IMAGES_PATH") or build_s3_path("input_data/images/raw_images")


def get_processed_images_dir() -> str:
    if ENVIRONMENT == "local_dev":
        return build_local_path(get("LOCAL_PROCESSED_IMAGE_DIR", "backend/output_data/processed_images"))
    return get("S3_PROCESSED_IMAGES_PATH") or build_s3_path("output_data/processed_images")