"""
===============================================================================
 NEXO BACKEND PACKAGE INITIALIZER
===============================================================================

Purpose:
--------
This file marks the 'backend' directory as a valid Python package and provides
a single, stable namespace for all internal modules such as:

    backend.services.pdf_processor
    backend.services.csv_utils
    backend.main

Why it's needed:
----------------
Python treats a directory as a package only if it contains __init__.py.
This ensures that relative and absolute imports work consistently
across FastAPI, CLI tools, and test environments.

Metadata:
---------
Author  : NEXO Systems Engineering Team
Version : 1.0.0
Date    : 31-Oct-2025
===============================================================================
"""

__version__ = "1.0.0"
__author__ = "NEXO Systems Engineering Team"

# Optional: lightweight bootstrap log (for debugging import issues)
import logging

logger = logging.getLogger("backend")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] [backend] %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

logger.debug("✅ backend package initialized successfully")