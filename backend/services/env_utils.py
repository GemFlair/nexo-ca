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
# S3 prefix policy: disable S3 in local_dev and map env-specific prefixes for higher environments.
if ENVIRONMENT == "local_dev":
    S3_BASE_PREFIX = None
else:
    env_prefix_default = {"dev": "dev", "prod": "prod"}
    default_prefix = env_prefix_default.get(ENVIRONMENT)
    S3_BASE_PREFIX = os.getenv("S3_BASE_PREFIX", default_prefix or "dev")

def build_s3_path(subpath: str) -> str:
    """
    Return a full S3 path, e.g.:
        s3://bucket/dev/input_data/pdf
    """
    # Guard to prevent accidental S3 usage when the environment is local-only.
    if S3_BASE_PREFIX is None:
        raise RuntimeError("S3 access disabled in local_dev environment")
    return f"s3://{BUCKET_NAME}/{S3_BASE_PREFIX}/{subpath.lstrip('/')}"

def build_local_path(subpath: str) -> str:
    """
    Return a local absolute path within the repo, e.g.:
        backend/input_data/pdf
    """
    base = repo_root
    return str(base / subpath)

def get_environment() -> str:
    """Return the current environment name (local_dev / dev / prod)."""
    return ENVIRONMENT

def get_bucket_prefix() -> str:
    """Return full S3 prefix for the active environment."""
    return f"{BUCKET_NAME}/{S3_BASE_PREFIX}"

def get(key: str, default=None) -> Union[str, None]:
    """Get an environment variable value."""
    return os.getenv(key, default)

# ===========================================================
# 📁 Path Resolution Helpers (PDF Processor)
# ===========================================================

def get_input_pdf_dir() -> str:
    """Get PDF input directory (environment-aware)."""
    if ENVIRONMENT == "local_dev":
        return build_local_path(get("LOCAL_INPUT_PDF_DIR", "backend/input_data/pdf") or "backend/input_data/pdf")
    else:
        return get("S3_INPUT_PDF_PATH") or build_s3_path("input_data/pdf")

def get_output_announcements_dir() -> str:
    """Get announcements output directory (environment-aware)."""
    if ENVIRONMENT == "local_dev":
        return build_local_path(get("LOCAL_ANNOUNCEMENTS_DIR", "output_data/announcements") or "output_data/announcements")
    else:
        return get("S3_OUTPUT_ANNOUNCEMENTS_PATH") or build_s3_path("output_data/announcements")

def get_processed_pdf_dir() -> str:
    """Get processed PDF directory (environment-aware)."""
    if ENVIRONMENT == "local_dev":
        return build_local_path(get("LOCAL_PROCESSED_PDF_DIR", "output_data/processed_pdf") or "output_data/processed_pdf")
    else:
        return get("S3_PROCESSED_PDF_PATH") or build_s3_path("output_data/processed_pdf")

def get_error_dir() -> str:
    """Get error reports directory (always local)."""
    return build_local_path(get("LOCAL_ERROR_DIR", "error_reports") or "error_reports")

def get_temp_dir() -> str:
    """Get temporary directory for processing (always local)."""
    return build_local_path(get("LOCAL_TEMP_DIR", "tmp") or "tmp")

def get_image_temp_dir() -> str:
    """Get temporary directory for image extraction (always local)."""
    return build_local_path(get("LOCAL_IMAGE_TEMP_DIR", "input_data/images/tmp") or "input_data/images/tmp")

def get_default_logo_path() -> str:
    """Get default company logo path."""
    return build_local_path(get("LOCAL_DEFAULT_LOGO_PATH", "input_data/images/defaults/Default_logo.png") or "input_data/images/defaults/Default_logo.png")

def get_default_banner_path() -> str:
    """Get default banner image path."""
    return build_local_path(get("LOCAL_DEFAULT_BANNER_PATH", "input_data/images/defaults/Default_banner.webp") or "input_data/images/defaults/Default_banner.webp")

# ===========================================================
# 📁 Path Resolution Helpers (CSV Processor)
# ===========================================================

def get_raw_csv_dir() -> str:
    """Get raw CSV input directory (environment-aware)."""
    if ENVIRONMENT == "local_dev":
        return build_local_path(get("LOCAL_RAW_CSV_DIR", "input_data/csv/eod_csv") or "input_data/csv/eod_csv")
    else:
        return get("S3_RAW_CSV_PATH") or build_s3_path("input_data/csv/eod_csv")

def get_processed_csv_dir() -> str:
    """Get processed CSV output directory (environment-aware)."""
    if ENVIRONMENT == "local_dev":
        return build_local_path(get("LOCAL_PROCESSED_CSV_DIR", "output_data/processed_csv") or "output_data/processed_csv")
    else:
        return get("S3_PROCESSED_CSV_PATH") or build_s3_path("output_data/processed_csv")

def get_static_csv_dir() -> str:
    """Get static CSV directory (environment-aware)."""
    if ENVIRONMENT == "local_dev":
        return build_local_path(get("LOCAL_STATIC_DIR", "input_data/csv/static") or "input_data/csv/static")
    else:
        return get("S3_STATIC_CSV_PATH") or build_s3_path("input_data/csv/static")

# ===========================================================
# 📁 Path Resolution Helpers (Image Processor)
# ===========================================================

def get_raw_images_dir() -> str:
    """Get raw images input directory (environment-aware)."""
    if ENVIRONMENT == "local_dev":
        return build_local_path(get("LOCAL_RAW_IMAGE_DIR", "input_data/images/raw_images") or "input_data/images/raw_images")
    else:
        return get("S3_RAW_IMAGES_PATH") or build_s3_path("input_data/images/raw_images")

def get_processed_images_dir() -> str:
    """Get processed images output directory (environment-aware)."""
    if ENVIRONMENT == "local_dev":
        return build_local_path(get("LOCAL_PROCESSED_IMAGE_DIR", "output_data/processed_images") or "output_data/processed_images")
    else:
        return get("S3_PROCESSED_IMAGES_PATH") or build_s3_path("output_data/processed_images")