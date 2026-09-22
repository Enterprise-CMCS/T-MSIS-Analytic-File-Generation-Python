"""
Unit tests for taf.FTX.FTX_Runner — the FTX orchestrator.
Covers: __init__, init().
"""


import pytest
from unittest.mock import MagicMock, patch


# ===========================================================================
# Constants & Fixtures
# ===========================================================================

#Reusable dictionary of default constructor arguments for FTX_Runner, shared across all tests in the file. 
RUNNER_ARGS = dict(
    da_schema="prod_schema",
    reporting_period="2024-03-01",
    state_code="CA",
    run_id="RUN_001",
    job_id=9999,
    file_version="9.0",
)


@pytest.fixture
def make_runner():
    """Factory: returns an FTX_Runner with TAF_Runner.__init__ mocked out."""
    with patch("taf.TAF_Runner.TAF_Runner.__init__", return_value=None) as mock_super:
        from taf.FTX.FTX_Runner import FTX_Runner

        def _create(run_stats_only=0, **overrides):
            args = {**RUNNER_ARGS, "run_stats_only": run_stats_only, **overrides}
            runner = FTX_Runner(**args)
            runner.logger = MagicMock()
            runner.append = MagicMock()
            runner.get_combined_list = MagicMock(return_value="'CA_RUN_001'")
            return runner, mock_super

        yield _create


# ===========================  __init__  ====================================
class TestFTXRunnerInit:

    # Verify FTX_Runner delegates all constructor args to TAF_Runner.__init__.
    def test_calls_super_init_with_all_args(self, make_runner):
        runner, mock_super = make_runner(run_stats_only=0)
        mock_super.assert_called_once_with(
            "prod_schema", "2024-03-01", "CA", "RUN_001", 9999, "9.0", 0
        )

    # Verify run_stats_only is stored as False when passed 0.
    def test_run_stats_only_false_when_0(self, make_runner):
        runner, _ = make_runner(run_stats_only=0)
        assert runner.run_stats_only is False

    # Verify run_stats_only is stored as True when passed 1.
    def test_run_stats_only_true_when_1(self, make_runner):
        runner, _ = make_runner(run_stats_only=1)
        assert runner.run_stats_only is True

    # Verify a ValueError is raised for invalid run_stats_only values.
    def test_run_stats_only_rejects_invalid_int(self, make_runner):
        with pytest.raises(ValueError, match="run_stats_only"):
            make_runner(run_stats_only=5)

    # Verify a custom da_schema is forwarded to TAF_Runner.__init__.
    def test_passes_custom_da_schema_to_super(self, make_runner):
        runner, mock_super = make_runner(da_schema="custom_schema")
        assert mock_super.call_args[0][0] == "custom_schema"

    # Verify a custom reporting_period is forwarded to TAF_Runner.__init__.
    def test_passes_custom_reporting_period_to_super(self, make_runner):
        runner, mock_super = make_runner(reporting_period="2025-01-01")
        assert mock_super.call_args[0][1] == "2025-01-01"

    # Verify a custom state_code is forwarded to TAF_Runner.__init__.
    #Implement fips code
    def test_passes_custom_state_code_to_super(self, make_runner):
        runner, mock_super = make_runner(state_code="NY")
        assert mock_super.call_args[0][2] == "NY"


# =============================  init()  ====================================
class TestFTXRunnerInitMethod:

    #The nine raw T-MSIS financial transaction table names
    EXPECTED_SEGMENTS = [
        "tmsis_indvdl_cptatn_pmpm",
        "tmsis_indvdl_hi_prm_pymt",
        "tmsis_grp_insrnc_prm_pymt",
        "tmsis_cst_shrng_ofst",
        "tmsis_val_bsd_pymt",
        "tmsis_sdp_seprt_pymt_term",
        "tmsis_cst_stlmt_pymt",
        "tmsis_fqhc_wrp_pymt",
        "tmsis_misc_pymt",
    ]

    @pytest.fixture
    def init_mocks(self, make_runner):
        """Run init() with FTX and TAF_Claims fully mocked."""
        runner, _ = make_runner()

        with patch("taf.FTX.FTX.FTX") as MockFTX, \
             patch("taf.FTX.FTX_Runner.TAF_Claims") as MockClaims:

            mock_ftx_instance = MagicMock()
            MockFTX.return_value = mock_ftx_instance

            mock_claims_instance = MagicMock()
            mock_claims_instance.rep_mo = 3
            mock_claims_instance.rep_yr = 2024
            MockClaims.return_value = mock_claims_instance

            runner.init()

            return runner, mock_ftx_instance, mock_claims_instance, MockFTX, MockClaims

    # Verify init() creates an FTX instance passing the runner as the argument.
    def test_creates_ftx_instance_with_self(self, init_mocks):
        runner, _, _, MockFTX, _ = init_mocks
        MockFTX.assert_called_once_with(runner)

    # Verify init() creates a TAF_Claims instance passing the runner as the argument.
    def test_creates_taf_claims_with_self(self, init_mocks):
        runner, _, _, _, MockClaims = init_mocks
        MockClaims.assert_called_once_with(runner)

    # Verify init() calls AWS_Extract_FTX_segment exactly nine times.
    def test_extracts_all_nine_segments(self, init_mocks):
        _, mock_ftx, _, _, _ = init_mocks
        assert mock_ftx.AWS_Extract_FTX_segment.call_count == 9

    # Verify every extraction call uses the dc_prod_data_sources_catalog.tmsis schema.
    def test_extract_uses_correct_schema(self, init_mocks):
        _, mock_ftx, _, _, _ = init_mocks
        for c in mock_ftx.AWS_Extract_FTX_segment.call_args_list:
            assert c[0][0] == "dc_prod_data_sources_catalog.tmsis"

    # Verify every extraction call passes "FTX" as the file type.
    def test_extract_uses_ftx_file_type(self, init_mocks):
        _, mock_ftx, _, _, _ = init_mocks
        for c in mock_ftx.AWS_Extract_FTX_segment.call_args_list:
            assert c[0][1] == "FTX"

    # Verify the nine extraction calls use the expected segment table names in order.
    def test_extract_passes_correct_segment_names(self, init_mocks):
        _, mock_ftx, _, _, _ = init_mocks
        called_segments = [c[0][3] for c in mock_ftx.AWS_Extract_FTX_segment.call_args_list]
        assert called_segments == self.EXPECTED_SEGMENTS

    # Verify every extraction call receives the correct reporting month and year.
    def test_extract_passes_rep_mo_and_rep_yr(self, init_mocks):
        _, mock_ftx, _, _, _ = init_mocks
        for c in mock_ftx.AWS_Extract_FTX_segment.call_args_list:
            assert c[0][6] == 3      # rep_mo
            assert c[0][7] == 2024   # rep_yr

    # Verify the first segment extraction uses cptatn_prd start/end date columns.
    def test_first_segment_uses_correct_dates(self, init_mocks):
        _, mock_ftx, _, _, _ = init_mocks
        first_call = mock_ftx.AWS_Extract_FTX_segment.call_args_list[0]
        assert first_call[0][4] == "cptatn_prd_strt_dt"
        assert first_call[0][5] == "cptatn_prd_end_dt"

    # Verify the last segment extraction uses pymt_prd start/end date columns.
    def test_last_segment_uses_correct_dates(self, init_mocks):
        _, mock_ftx, _, _, _ = init_mocks
        last_call = mock_ftx.AWS_Extract_FTX_segment.call_args_list[-1]
        assert last_call[0][4] == "pymt_prd_strt_dt"
        assert last_call[0][5] == "pymt_prd_end_dt"

    # Verify stack_segments is called exactly once during init().
    def test_stack_segments_called_once(self, init_mocks):
        _, mock_ftx, _, _, _ = init_mocks
        mock_ftx.stack_segments.assert_called_once()

    # Verify stack_segments receives a dict keyed by all nine segment names.
    def test_stack_segments_receives_all_nine_keys(self, init_mocks):
        _, mock_ftx, _, _, _ = init_mocks
        passed_dict = mock_ftx.stack_segments.call_args[0][0]
        assert list(passed_dict.keys()) == self.EXPECTED_SEGMENTS

    # Verify create() is called once with the runner instance.
    def test_create_called_with_runner(self, init_mocks):
        runner, mock_ftx, _, _, _ = init_mocks
        mock_ftx.create.assert_called_once_with(runner)

    # Verify build() is called once with the runner instance.
    def test_build_called_with_runner(self, init_mocks):
        runner, mock_ftx, _, _, _ = init_mocks
        mock_ftx.build.assert_called_once_with(runner)
    
    # Asserts the exact call sequence
    def test_method_call_order(self, init_mocks):
        """Verify extract -> stack -> create -> build ordering."""
        _, mock_ftx, _, _, _ = init_mocks
        expected_order = (
            ["AWS_Extract_FTX_segment"] * 9
            + ["stack_segments", "create", "build"]
        )
        actual_order = [c[0] for c in mock_ftx.method_calls]
        assert actual_order == expected_order
