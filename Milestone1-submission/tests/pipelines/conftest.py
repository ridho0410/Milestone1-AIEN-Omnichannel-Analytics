"""Pytest configuration for the pipeline tests.

The data generator lives in the instructor package (``solution/``) and is not
part of the participant distribution, so its test module is skipped when the
generator is unavailable instead of failing collection.
"""

from __future__ import annotations

import importlib.util

if importlib.util.find_spec("pipelines.generator") is None:
    collect_ignore = ["test_generator.py"]
