# NEXO Backend - AI Coding Assistant Instructions

## Project Overview

NEXO is a FastAPI-based backend service for processing financial market data, specifically CSV files containing stock market information. The system processes raw CSV data, transforms it into structured market snapshots, and serves announcements with AI-generated summaries and sentiment analysis.

### Core Architecture

**Service Boundaries:**
- **CSV Processor** (`backend/services/csv_processor.py`): Handles CSV ingestion, transformation, and S3/local storage with automatic fallbacks
- **PDF Processor** (`backend/services/pdf_processor.py`): Processes PDF announcements, extracts text, generates AI summaries and sentiment badges
- **Image Processor** (`backend/services/image_utils.py`): Handles logo/banner extraction from PDFs
- **LLM Utils** (`backend/services/llm_utils.py`): OpenAI integration for text analysis and summarization
- **Routers**: REST API endpoints for announcements, health checks, and raw data access

**Data Flow:**
1. CSV files → `csv_processor` → Market snapshots with rankings and metrics
2. PDF announcements → `pdf_processor` → JSON metadata with AI summaries
3. API requests → `routers` → Pydantic-validated responses

**Key Design Patterns:**
- **S3-First with Local Fallback**: All storage operations prefer S3 but gracefully fall back to local filesystem
- **Async Context Preservation**: FastAPI endpoints use `contextvars` to maintain request context in thread pools
- **Lazy Settings Loading**: Environment variables loaded only when needed, never at import time
- **Thread-Safe Caching**: Double-checked locking for settings and filesystem objects

## Critical Developer Workflows

### Environment Setup
```bash
# Required for S3 operations
pip install fsspec s3fs boto3

# Load environment variables before importing services
from dotenv import load_dotenv
load_dotenv()

# Validate configuration at startup
from backend.services.csv_processor import preload_settings, validate_settings
preload_settings()  # Warm caches
validate_settings(allow_local_default=False)  # Fail fast in production
```

### Testing
```bash
# Unit tests with S3 mocking
cd backend && python -m pytest tests/ -v

# Integration tests
python -m pytest tests/integration/ -v

# Run CSV processor CLI
python -m backend.services.csv_processor --verbose
```

### Deployment
- **Container**: Use provided Dockerfile with Python 3.13-slim
- **Environment Variables**: Set `S3_RAW_CSV_PATH`, `S3_PROCESSED_CSV_PATH`, `S3_STORAGE_OPTIONS`
- **Metrics**: Prometheus integration at `/metrics` endpoint
- **Logging**: JSON format in production, human-readable in development

## Project-Specific Conventions

### Error Handling
- **Graceful Degradation**: S3 failures increment metrics but don't crash - fallback to local storage
- **Validation First**: Check required columns/data before processing
- **Context Preservation**: Use `contextvars` for async operations to maintain request IDs

### Data Transformation Patterns
```python
# Column mapping with fallbacks
out["symbol"] = df.get("Symbol")
out["company_name"] = df.get("Description")

# Percentage parsing (remove % and convert)
out["change_1d_pct"] = _col_or_none(df, "Price Change % 1 day").map(
    lambda x: float(str(x).replace("%", "").strip()) if x else None
)

# Currency conversion (rupees to crores)
out["mcap_rs_cr"] = _col_or_none(df, "Market capitalization").map(
    lambda v: round(v / 1e7, 2) if v else None
)
```

### Logging Patterns
- **Request Context**: All logs include `request_id` from context variables
- **Security**: Sensitive config values redacted in logs using `_sanitize_config()`
- **Metrics Integration**: Fallback events logged with counters for observability

### File Organization
- **Services**: Business logic in `backend/services/` with clear separation (CSV, PDF, LLM, images)
- **Routers**: API endpoints in `backend/routers/` with Pydantic schemas
- **Tests**: Comprehensive test suites in `backend/tests/` and `tests/integration/`
- **Data**: Processed JSON in `data/announcements/`, static files in `input_data/`

## Integration Points

### External Dependencies
- **AWS S3**: Primary storage with `fsspec` + `s3fs` + `boto3`
- **OpenAI**: Text analysis and summarization
- **Prometheus**: Metrics collection
- **Pydantic**: Settings and validation (optional dependency with fallbacks)

### Cross-Component Communication
- **Settings Sharing**: All services use shared `CSVSettings` from `csv_processor`
- **Metrics Injection**: External collectors can be injected via `set_metrics_collector()`
- **File Discovery**: Services discover latest files using date-based naming patterns

## Key Files and Examples

### Core Processing Logic
- `backend/services/csv_processor.py`: S3-aware CSV processing with fallbacks
- `backend/services/pdf_processor.py`: PDF text extraction and AI enrichment
- `backend/main.py`: FastAPI app with middleware, CORS, and lifespan management

### Configuration Examples
```python
# S3 Configuration
S3_RAW_CSV_PATH=s3://bucket/raw/
S3_PROCESSED_CSV_PATH=s3://bucket/processed/
S3_STORAGE_OPTIONS='{"key": "access_key", "secret": "secret", "endpoint_url": "https://s3.example.com"}'

# Processing Settings
CSV_THREADPOOL_SIZE=4
LOG_LEVEL=INFO
```

### API Usage Patterns
```python
# Synchronous processing
result = process_csv_sync("path/to/file.csv")
# Returns: {"path": "/output/file.csv", "s3": False, "rows": 100}

# Async processing (FastAPI)
@app.post("/process")
async def process_endpoint(file: UploadFile):
    result = await async_process_csv(file.filename)
    return result
```

## Common Patterns to Follow

- **Always handle S3 failures gracefully** - increment metrics and fallback to local
- **Validate data early** - check required columns before expensive operations
- **Use lazy loading** - don't load settings or create connections at import time
- **Preserve async context** - copy `contextvars` when delegating to thread pools
- **Log sanitization** - redact sensitive values before logging configuration
- **Thread safety** - use locks for shared mutable state
- **Fail fast in production** - validate critical config at startup

## Testing Priorities

Focus tests on:
- S3 fallback behavior and metrics incrementing
- Async context preservation
- Configuration validation and error handling
- Data transformation accuracy (column mappings, rounding, sorting)
- Thread pool executor lifecycle management

This codebase prioritizes reliability, observability, and graceful degradation over raw performance, making it suitable for financial data processing where data integrity is paramount.