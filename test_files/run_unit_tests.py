

from __future__ import annotations

import sys
import unittest
from pathlib import Path


DEFAULT_TESTS_ROOT = Path(
    "/Workspace/Users/nirendraprabhu750@gmail.com/weather-rt/test_files"
)


def _get_tests_root() -> Path:
    """Return the tests folder path, preferring the Databricks workspace location."""
    if DEFAULT_TESTS_ROOT.exists():
        return DEFAULT_TESTS_ROOT
    return Path.cwd().resolve()


def main() -> int:
    """Discover and run all test_*.py files under the tests folder.

    Input: no arguments; it reads tests from the configured workspace path.
    Output: process exit code 0 for success, 1 for failure.
    """
    tests_root = _get_tests_root()

    if not tests_root.exists():
        raise FileNotFoundError(f"Tests folder not found: {tests_root}")

    # Allow imports like: import weather_pipeline_helpers
    if str(tests_root) not in sys.path:
        sys.path.insert(0, str(tests_root))

    # Discover tests
    loader = unittest.TestLoader()
    suite = loader.discover(start_dir=str(tests_root), pattern="test_*.py")
    
    # Run tests with verbosity
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())