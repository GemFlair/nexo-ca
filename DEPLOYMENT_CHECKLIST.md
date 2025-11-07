# CSV Processor v7.1 Deployment Checklist

## Pre-Deployment (1 hour)

### 1. Apply Python <3.9 Compatibility Fix
```python
# In shutdown_csv_executors() (line ~776)
try:
    _threadpool.shutdown(wait=True, cancel_futures=True)
except TypeError:  # Python <3.9
    _threadpool.shutdown(wait=True)
```

### 2. Add Correlation ID to Health Check
```python
# In health_check() (line ~788)
return {
    "status": "healthy",
    "correlation_id": correlation_id_var.get(),  # ADD THIS
    # ... rest
}
```

### 3. Simplify S3 Client Creation
```python
# In _write_s3_csv_with_retry() (line ~464)
session = boto3.session.Session(profile_name=opts["profile"]) if "profile" in opts else boto3
client = session.client("s3", config=conf, endpoint_url=opts.get("endpoint_url"))
```

## Testing Phase (2 hours)

### 4. Run Existing Test Suite
```bash
cd backend
python -m pytest tests/test_csv_processor_comprehensive.py -v
```
**Expected**: All tests pass (especially `test_s3_write_fallback_to_local`)

### 5. Integration Test with FastAPI
```bash
# Start server
python -m backend.main

# Verify startup logs show:
# "CSV settings initialized"
# "CSV processor settings validated"
```

### 6. Test Async Endpoint
```python
# In main.py route handler
result = await csv_processor.async_process_csv("test.csv")
assert result["error"] is None  # S3 success
# OR
assert result["error"] is not None  # S3 fallback case
```

## Deployment Steps (30 minutes)

### 7. Backup Current Version
```bash
cp backend/services/csv_processor.py backend/services/csv_processor.py.backup-$(date +%Y%m%d)
```

### 8. Deploy New Version
```bash
# Copy reviewed code to csv_processor.py
git add backend/services/csv_processor.py
git commit -m "feat: enterprise CSV processor v7.1 - unified retry/fallback"
git push origin dev-playground
```

### 9. Verify in Staging
- Check `/health` endpoint shows version `7.1.0-unified`
- Monitor logs for correlation IDs
- Verify metrics in `get_performance_metrics()`

### 10. Production Rollout
```bash
git checkout main
git merge dev-playground
git tag v7.1.0
git push origin main --tags
```

## Post-Deployment Monitoring (24 hours)

### 11. Watch Key Metrics
```python
# Prometheus/custom metrics dashboard
csv_fallback_count{job="nexo-backend"}  # Should be <1% of requests
csv_error_count{job="nexo-backend"}     # Should be ~0
csv_p95_duration_s{job="nexo-backend"}  # Should be <5s
```

### 12. Alert Thresholds
- **Fallback rate >5%**: Investigate S3 connectivity
- **Error rate >1%**: Check logs for validation failures
- **p95 latency >10s**: Review CSV sizes and chunk settings

### 13. Rollback Plan
```bash
# If issues detected:
git revert HEAD
git push origin main
# Redeploy previous version
```

## Configuration Validation

### Required Env Vars (Production)
```bash
# S3 paths (mandatory for prod)
S3_RAW_CSV_PATH=s3://nexo-prod-data/raw-csvs/
S3_PROCESSED_CSV_PATH=s3://nexo-prod-data/processed-csvs/

# Credentials (via Secrets Manager preferred)
AWS_S3_SECRETS_NAME=prod/nexo/s3-credentials

# Behavior
FORCE_PROCESSED_TO_S3=true
S3_RAW_ONLY=false  # Allow local fallback
CSV_SANITIZE_OUTPUT=true  # Prevent Excel formula injection

# Logging
LOG_FORMAT=json
LOG_LEVEL=INFO
```

### Optional Tuning
```bash
# Performance
CSV_THREADPOOL_SIZE=8  # Increase for high concurrency
CSV_CHUNK_SIZE=10000   # Larger for big files
S3_RETRIES=3           # More aggressive retry
S3_RETRY_BACKOFF=2.0   # Faster backoff

# Limits
MAX_CSV_SIZE_MB=200    # Increase if needed
S3_OP_TIMEOUT=60       # Longer for slow networks
```

## Success Criteria

✅ All existing tests pass  
✅ FastAPI startup completes without errors  
✅ Health check returns `{"status": "healthy"}`  
✅ Async processing works in routes  
✅ Fallback mechanism tested (simulate S3 failure)  
✅ Correlation IDs appear in structured logs  
✅ Metrics counters increment correctly  
✅ Graceful shutdown completes within 5s  

## Rollback Triggers

🚨 **Immediately rollback if:**
- Test suite failure rate >10%
- Production error rate >5% after 1 hour
- FastAPI startup failures
- Data integrity issues (hash mismatches)
- Memory leaks (RSS growth >20% in 1 hour)

## Documentation Updates

- [ ] Update API documentation with v7.1 changes
- [ ] Add migration guide for csv_utils.py consumers
- [ ] Document new configuration options
- [ ] Update Prometheus metrics catalog
- [ ] Add troubleshooting guide for common errors

---

**Estimated Total Time**: 3.5 hours  
**Risk Level**: Low (all breaking changes mitigated)  
**Rollback Complexity**: Trivial (single file)
