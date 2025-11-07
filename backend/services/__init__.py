"""
Lightweight services package initializer.

Dual-Mode Support:
- Core Mode (default): Core API modules are eagerly imported and accessible.
- Full Mode: Set SERVICES_FULL_MODE=1 to enable lazy access to dev-only submodules.
"""

from __future__ import annotations
import importlib
import os
import types
from typing import Optional, Dict

# --- Configuration ---
FULL_MODE = os.getenv("SERVICES_FULL_MODE", "0").strip().lower() in ("1", "true", "yes")
_core_allowed = ("csv_utils", "image_utils", "pdf_processor", "index_builder", "llm_utils", "sentiment_utils")
_dev_only = ("image_processor", "csv_processor", "filename_utils", "pdf_watcher", "update_logo_and_patch_json")

_import_cache: Dict[str, Optional[types.ModuleType]] = {}


def load_env() -> None:
    """
    Delegates environment loading to backend.services.env_utils.

    env_utils handles all .env logic centrally, including file discovery and loading.
    """
    from backend.services import env_utils
    env_utils.get_environment()


def _lazy_import(name: str) -> Optional[types.ModuleType]:
    """Attempt lazy import via several fallback paths."""
    if name in _import_cache:
        return _import_cache[name]

    candidates = [
        f"{__name__}.{name}",
        f"services.{name}",
        name,
    ]

    mod = None
    for candidate in candidates:
        try:
            mod = importlib.import_module(candidate)
            break
        except Exception:
            continue

    _import_cache[name] = mod
    globals()[name] = mod
    return mod


# --- Eagerly import core modules so they're available at package import time ---
# This ensures "from backend.services import csv_utils" works correctly
csv_utils = _lazy_import("csv_utils")
image_utils = _lazy_import("image_utils")
pdf_processor = _lazy_import("pdf_processor")
index_builder = _lazy_import("index_builder")
llm_utils = _lazy_import("llm_utils")
sentiment_utils = _lazy_import("sentiment_utils")

__all__ = ["load_env", "csv_utils", "image_utils", "pdf_processor", "index_builder", "llm_utils", "sentiment_utils"]


def __getattr__(attr: str) -> Optional[types.ModuleType]:
    """
    Lazy attribute resolver for dev-only modules.

    - Core modules are already imported above.
    - In full mode, dev-only modules are accessible via lazy loading.
    - Raises AttributeError if module not allowed or fails to import.
    """
    # If already imported (core modules), return from globals
    if attr in globals():
        return globals()[attr]

    allowed = _core_allowed + _dev_only if FULL_MODE else _core_allowed

    if attr not in allowed:
        raise AttributeError(
            f"module '{__name__}' has no attribute '{attr}' "
            "(core mode; set SERVICES_FULL_MODE=1 for full access)"
        )

    mod = _lazy_import(attr)
    if attr in _core_allowed and mod is None:
        raise AttributeError(
            f"core module '{attr}' could not be imported (check for missing deps or syntax errors)"
        )
    return mod


def __dir__():
    """Dynamic autocomplete for IDEs (core + dev depending on mode)."""
    base = set(globals().keys())
    allowed = _core_allowed + _dev_only if FULL_MODE else _core_allowed
    base.update(allowed)
    return sorted(base)
