"""
Shared pytest fixtures for the TAF FTX test suite.

Provides:
    - make_ftx:    yields (FTX instance, mock_runner) with TAF.__init__ patched out.
    - make_runner: factory that returns an FTX_Runner with TAF_Runner.__init__ mocked.
    - meta:        returns the FTX_Metadata class for static-method testing.

Place this file at  test/conftest.py  so every file under test/ can use these
fixtures without importing them.
"""

import pytest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
#  FTX (the SQL-builder class)
# ---------------------------------------------------------------------------

@pytest.fixture
def make_ftx():
    """
    Yield (ftx, runner) where:
      - ftx   is an FTX instance with TAF.__init__ bypassed
      - runner is a MagicMock pre-loaded with the attributes FTX methods read

    Used by: test_ftx.py  (TestAWSExtractFTXSegment, TestStackSegments,
             TestCreate, TestBuild)
    """
    with patch("taf.TAF.TAF.__init__", return_value=None):
        from taf.FTX.FTX import FTX

        runner = MagicMock()
        runner.DA_RUN_ID = 5001
        runner.VERSION = "9.0"
        runner.TAF_FILE_DATE = "2024-03-01"
        runner.DA_SCHEMA = "prod_schema"
        runner.run_stats_only = False
        runner.get_combined_list.return_value = "'CA_RUN_001'"
        runner.logger = MagicMock()

        ftx = FTX(runner)
        yield ftx, runner


# ---------------------------------------------------------------------------
#  FTX_Runner (the orchestrator)
# ---------------------------------------------------------------------------

RUNNER_DEFAULTS = dict(
    da_schema="prod_schema",
    reporting_period="2024-03-01",
    state_code="CA",
    run_id="RUN_001",
    job_id=9999,
    file_version="9.0",
)


@pytest.fixture
def make_runner():
    """
    Factory fixture: call _create() to get (runner, mock_super).

    Used by: test_ftx_runner.py  (TestFTXRunnerInit, TestFTXRunnerInitMethod)
    """
    with patch("taf.TAF_Runner.TAF_Runner.__init__", return_value=None) as mock_super:
        from taf.FTX.FTX_Runner import FTX_Runner

        def _create(run_stats_only=0, **overrides):
            args = {**RUNNER_DEFAULTS, "run_stats_only": run_stats_only, **overrides}
            runner = FTX_Runner(**args)
            runner.logger = MagicMock()
            runner.append = MagicMock()
            runner.get_combined_list = MagicMock(return_value="'CA_RUN_001'")
            return runner, mock_super

        yield _create


# ---------------------------------------------------------------------------
#  FTX_Metadata (static column / cleanser / rename definitions)
# ---------------------------------------------------------------------------

@pytest.fixture
def meta():
    """
    Return the FTX_Metadata class itself (all methods are static/class-level).

    Used by: test_ftx_metadata.py
    """
    from taf.FTX.FTX_Metadata import FTX_Metadata
    return FTX_Metadata
