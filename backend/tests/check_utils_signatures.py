#!/usr/bin/env python3
# backend/tests/check_utils_signatures.py
# Run from repo root: python backend/tests/check_utils_signatures.py
import importlib
import inspect
import json
import sys
import tempfile
from pathlib import Path

MODULES = {
    "aws_utils": ["download_file_from_s3", "upload_file_to_s3"],
    "csv_utils": ["get_market_snapshot_full", "get_market_snapshot", "get_indices_for_symbol"],
    "filename_utils": ["filename_to_symbol"],
    "image_utils": ["process_and_upload_image", "detect_role_for_images"],
    "llm_utils": ["async_classify_headline_and_summary", "classify_headline_and_summary", "async_classify_sentiment", "classify_sentiment"],
    "sentiment_utils": ["compute_sentiment"],
    "resilience_utils": ["retry_sync", "get_circuit_breaker"],
    "observability_utils": ["metrics_inc", "audit_log", "get_logger"],
}

SAFE_TEST_CALLS = {
    # module: {function: (args_tuple, kwargs_dict, should_attempt_call_bool)}
    "filename_utils": {
        "filename_to_symbol": (("Bajaj Finserv Ltd_26-09-2025.pdf",), {}, True)
    },
    "csv_utils": {
        "get_market_snapshot_full": (("BAJAJ",), {}, True), # safe local lookup expected
        "get_market_snapshot": (("BAJAJ",), {}, True)
    },
    "sentiment_utils": {
        "compute_sentiment": (("This is a positive corporate announcement.",), {}, True)
    }
}

def import_mod(name):
    try:
        return importlib.import_module(f"backend.services.{name}")
    except Exception:
        try:
            return importlib.import_module(name)
        except Exception:
            return None

def safe_sig(obj):
    try:
        return str(inspect.signature(obj))
    except Exception:
        return "<no-signature>"

def attempt_call(fn, args, kwargs):
    try:
        res = fn(*args, **kwargs)
        # if coroutine function, don't await; just report coroutine type
        if inspect.iscoroutine(res):
            return {"call_ok": True, "returned": "coroutine", "type": str(type(res))}
        return {"call_ok": True, "returned_type": type(res).__name__, "repr": (res if isinstance(res, (dict, list, tuple, str, int, float, bool)) else str(type(res)))}
    except Exception as e:
        return {"call_ok": False, "error": repr(e)}

def check_module(name, funcs):
    mod = import_mod(name)
    out = {"module": name, "found": bool(mod), "functions": {}}
    if not mod:
        return out
    for fn in funcs:
        obj = getattr(mod, fn, None)
        if obj is None:
            out["functions"][fn] = {"present": False}
            continue
        info = {"present": True, "signature": safe_sig(obj)}
        # safe test calls only for whitelisted functions
        if name in SAFE_TEST_CALLS and fn in SAFE_TEST_CALLS[name]:
            args, kwargs, do_call = SAFE_TEST_CALLS[name][fn]
            if do_call:
                info["test_call"] = attempt_call(obj, args, kwargs)
        out["functions"][fn] = info
    return out

def pretty_print(result):
    print("="*80)
    print("Module:", result["module"], "| Found:", result["found"])
    if not result["found"]:
        print("  -> module missing")
        return
    for fn, meta in result["functions"].items():
        print(f"  - {fn}: present={meta.get('present', False)}")
        if meta.get("present"):
            print(f"      signature: {meta.get('signature')}")
            if "test_call" in meta:
                tc = meta["test_call"]
                if tc.get("call_ok"):
                    print(f"      test_call: OK -> returned_type={tc.get('returned_type', tc.get('returned'))}")
                    if "repr" in tc and isinstance(tc["repr"], (str, int, float, bool)):
                        print(f"        repr/sample: {tc['repr']}")
                else:
                    print(f"      test_call: FAILED -> error={tc.get('error')}")
    print("="*80 + "\n")

def main():
    results = []
    for mod, funcs in MODULES.items():
        res = check_module(mod, funcs)
        results.append(res)
        pretty_print(res)
    # Summarize expected shapes guidance
    guidance = {
        "aws_utils.download_file_from_s3": "Should write to local dest path; return True/False or raise on failure.",
        "aws_utils.upload_file_to_s3": "Should return True/False.",
        "csv_utils.get_market_snapshot_full": "Should return dict. Prefer fields 'logo_url' / 'banner_url' as str or list.",
        "filename_utils.filename_to_symbol": "Should return dict like {'found':True,'symbol':'BAJAJ','company_name':'Bajaj Finserv Ltd','score':0.95,'match_type':'best'}",
        "image_utils.process_and_upload_image": "Should accept local path and return {'local_path':..., 's3_url':...} or similar dict.",
        "llm_utils.*": "Prefer async functions; return dict or tuple: (headline, summary, meta) or {'headline_final':..., 'summary_60':..., 'llm_meta':...}",
        "sentiment_utils.compute_sentiment": "Should return dict with normalized 'label' and numeric 'score'."
    }
    print("Guidance (expected shapes):")
    print(json.dumps(guidance, indent=2))
    # Exit code: 0 if all modules found, else 2
    missing = [r for r in results if not r["found"]]
    if missing:
        print("Missing modules:", [m["module"] for m in missing])
        sys.exit(2)
    print("All listed modules found (signatures may still differ). Review printed signatures and test call outputs above.")
    sys.exit(0)

if __name__ == "__main__":
    main()