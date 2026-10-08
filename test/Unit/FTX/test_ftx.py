"""
Unit tests for taf.FTX.FTX — the FTX SQL builder class.
Covers: __init__, AWS_Extract_FTX_segment, stack_segments, create, build.
"""


import pytest
from unittest.mock import MagicMock, patch
from collections import OrderedDict



# ===========================  __init__  ====================================
class TestFTXInit:

    @patch("taf.TAF.TAF.__init__", return_value=None)
    # Verify FTX.__init__ sets st_fil_type to "FTX".
    def test_sets_st_fil_type(self, mock_super):
        from taf.FTX.FTX import FTX
        ftx = FTX(MagicMock())
        assert ftx.st_fil_type == "FTX"

    @patch("taf.TAF.TAF.__init__", return_value=None)
    # Verify FTX.__init__ delegates to TAF.__init__ with the runner.
    def test_calls_super_init_with_runner(self, mock_super):
        from taf.FTX.FTX import FTX
        runner = MagicMock()
        FTX(runner)
        mock_super.assert_called_once_with(runner)


# ==================  AWS_Extract_FTX_segment  ==============================
class TestAWSExtractFTXSegment:

    @pytest.fixture(autouse=True)
    def _mock_metadata(self):
        with patch(
            "taf.FTX.FTX_Metadata.FTX_Metadata.selectDataElements",
            return_value="a.COL1\n\t\t\t,a.COL2",
        ):
            yield

#Resusable dictionary of test inputs shared across all tests in AWSExtractFTXSegment
    EXTRACT_ARGS = dict(
        TMSIS_SCHEMA="tmsis_schema",
        fl="FTX",
        tab_no="CIP",
        _2x_segment="TMSIS_CIP_LANGR",
        analysis_date_start='"a.SRVC_BGN_DT"',
        analysis_date_end='"a.SRVC_END_DT"',
        rep_mo=6,
        rep_yr=2024,
    )

    def _get_sql(self, runner, index):
        return runner.append.call_args_list[index][0][1]

    # Verify extraction produces exactly three SQL statements.
    def test_appends_three_sql_statements(self, make_ftx):
        ftx, runner = make_ftx
        ftx.AWS_Extract_FTX_segment(**self.EXTRACT_ARGS)
        assert runner.append.call_count == 3

    # Verify all appended statements use "FTX" as the key.
    def test_all_appends_keyed_as_ftx(self, make_ftx):
        ftx, runner = make_ftx
        ftx.AWS_Extract_FTX_segment(**self.EXTRACT_ARGS)
        for c in runner.append.call_args_list:
            assert c[0][0] == "FTX"

    # -- First SQL: _IN view --
    # Verify first SQL creates the _IN temporary view.
    def test_creates_input_view(self, make_ftx):
        ftx, runner = make_ftx
        ftx.AWS_Extract_FTX_segment(**self.EXTRACT_ARGS)
        sql = self._get_sql(runner, 0)
        assert "create or replace temporary view TMSIS_CIP_LANGR_IN" in sql

    # Verify the _IN view queries the correct schema and segment table.
    def test_references_correct_schema_and_segment(self, make_ftx):
        ftx, runner = make_ftx
        ftx.AWS_Extract_FTX_segment(**self.EXTRACT_ARGS)
        sql = self._get_sql(runner, 0)
        assert "tmsis_schema.TMSIS_CIP_LANGR" in sql

    # Verify the _IN view filters on TMSIS_ACTV_IND = true.
    def test_filters_active_indicator(self, make_ftx):
        ftx, runner = make_ftx
        ftx.AWS_Extract_FTX_segment(**self.EXTRACT_ARGS)
        sql = self._get_sql(runner, 0)
        assert "TMSIS_ACTV_IND = true" in sql

    # Verify the _IN view excludes records with ADJSTMT_IND = '1'.
    def test_excludes_adjstmt_ind_1(self, make_ftx):
        ftx, runner = make_ftx
        ftx.AWS_Extract_FTX_segment(**self.EXTRACT_ARGS)
        sql = self._get_sql(runner, 0)
        assert "ADJSTMT_IND <> '1'" in sql

    # Verify the _IN view filters by reporting year and month.
    def test_date_filter_uses_rep_yr_and_rep_mo(self, make_ftx):
        ftx, runner = make_ftx
        ftx.AWS_Extract_FTX_segment(**self.EXTRACT_ARGS)
        sql = self._get_sql(runner, 0)
        assert "= 2024" in sql
        assert "= 6" in sql

    # Verify analysis date args are unquoted and coalesced correctly.
    def test_analysis_date_quotes_stripped(self, make_ftx):
        ftx, runner = make_ftx
        ftx.AWS_Extract_FTX_segment(**self.EXTRACT_ARGS)
        sql = self._get_sql(runner, 0)
        assert "coalesce(a.SRVC_END_DT,a.SRVC_BGN_DT)" in sql

    # -- Second SQL: _nodups view --
    # Verify second SQL creates the _nodups temporary view.
    def test_creates_nodups_view(self, make_ftx):
        ftx, runner = make_ftx
        ftx.AWS_Extract_FTX_segment(**self.EXTRACT_ARGS)
        sql = self._get_sql(runner, 1)
        assert "create or replace temporary view TMSIS_CIP_LANGR_nodups" in sql

    # Verify dedup logic retains only rows with exactly one TMSIS_RUN_ID.
    def test_dedup_uses_having_count_1(self, make_ftx):
        ftx, runner = make_ftx
        ftx.AWS_Extract_FTX_segment(**self.EXTRACT_ARGS)
        sql = self._get_sql(runner, 1)
        assert "having count(TMSIS_RUN_ID) = 1" in sql

    # Verify dedup coalesces PMT_OR_RCPMT_DT with a 1960-01-01 default.
    def test_dedup_coalesces_pmt_date_with_default(self, make_ftx):
        ftx, runner = make_ftx
        ftx.AWS_Extract_FTX_segment(**self.EXTRACT_ARGS)
        sql = self._get_sql(runner, 1)
        assert "coalesce(nodups.PMT_OR_RCPMT_DT, to_date('1960-01-01'))" in sql

    # -- Third SQL: ALL_ view with claims family join --
    # Verify third SQL creates the ALL_ temporary view.
    def test_creates_all_segment_view(self, make_ftx):
        ftx, runner = make_ftx
        ftx.AWS_Extract_FTX_segment(**self.EXTRACT_ARGS)
        sql = self._get_sql(runner, 2)
        assert "create or replace temporary view ALL_TMSIS_CIP_LANGR" in sql

    # Verify the ALL_ view joins to the claims family table.
    def test_joins_claims_family_table(self, make_ftx):
        ftx, runner = make_ftx
        ftx.AWS_Extract_FTX_segment(**self.EXTRACT_ARGS)
        sql = self._get_sql(runner, 2)
        assert "tmsis_clm_fmly_CIP" in sql

    # Verify the ALL_ view filters on clm_fmly_finl_actn_ind = true.
    def test_filters_final_action_indicator(self, make_ftx):
        ftx, runner = make_ftx
        ftx.AWS_Extract_FTX_segment(**self.EXTRACT_ARGS)
        sql = self._get_sql(runner, 2)
        assert "clm_fmly_finl_actn_ind  = true" in sql

    # Verify the ALL_ view includes the segment number as a literal.
    def test_adds_segment_number_literal(self, make_ftx):
        ftx, runner = make_ftx
        ftx.AWS_Extract_FTX_segment(**self.EXTRACT_ARGS)
        sql = self._get_sql(runner, 2)
        assert "'CIP' as TMSIS_SGMT_NUM" in sql

    # Verify the ALL_ view derives INDVDL_BENE_IND from MSIS_IDENT_NUM.
    def test_adds_individual_beneficiary_indicator(self, make_ftx):
        ftx, runner = make_ftx
        ftx.AWS_Extract_FTX_segment(**self.EXTRACT_ARGS)
        sql = self._get_sql(runner, 2)
        assert "INDVDL_BENE_IND" in sql
        assert "MSIS_IDENT_NUM is not null" in sql


# =======================  stack_segments  ==================================
class TestStackSegments:

    # Verify stack_segments creates the COMBINED_FTX view.
    def test_creates_combined_ftx_view(self, make_ftx):
        ftx, runner = make_ftx
        ftx.stack_segments({"TABLE_A": ["v"]})
        sql = runner.append.call_args[0][1]
        assert "create or replace temporary view COMBINED_FTX" in sql

    # Verify a single table produces no UNION ALL.
    def test_single_table_no_union(self, make_ftx):
        ftx, runner = make_ftx
        ftx.stack_segments({"TABLE_A": ["v"]})
        sql = runner.append.call_args[0][1]
        assert "ALL_TABLE_A" in sql
        assert "union all" not in sql.lower()

    # Verify N tables produce exactly N-1 UNION ALL clauses.
    def test_multiple_tables_have_correct_union_count(self, make_ftx):
        ftx, runner = make_ftx
        tables = OrderedDict([("T_A", []), ("T_B", []), ("T_C", [])])
        ftx.stack_segments(tables)
        sql = runner.append.call_args[0][1]
        assert sql.lower().count("union all") == 2

    # Verify all table names appear in the stacked SQL output.
    def test_all_table_names_present(self, make_ftx):
        ftx, runner = make_ftx
        tables = OrderedDict([("T_A", []), ("T_B", [])])
        ftx.stack_segments(tables)
        sql = runner.append.call_args[0][1]
        assert "ALL_T_A" in sql
        assert "ALL_T_B" in sql

    # Verify stack_segments calls runner.append exactly once.
    def test_calls_runner_append_once(self, make_ftx):
        ftx, runner = make_ftx
        ftx.stack_segments({"T_A": [], "T_B": []})
        assert runner.append.call_count == 1


# ============================  create  =====================================
class TestCreate:

    # Verify create() builds the FTX temporary view.
    def test_creates_ftx_view(self, make_ftx):
        ftx, runner = make_ftx
        ftx.create(runner)
        sql = runner.append.call_args[0][1]
        assert "create or replace temporary view FTX" in sql

    # Verify the FTX view includes DA_RUN_ID from the runner.
    def test_includes_da_run_id(self, make_ftx):
        ftx, runner = make_ftx
        ftx.create(runner)
        sql = runner.append.call_args[0][1]
        assert "5001 as DA_RUN_ID" in sql

    # Verify the FTX view includes the TAF version string.
    def test_includes_version(self, make_ftx):
        ftx, runner = make_ftx
        ftx.create(runner)
        sql = runner.append.call_args[0][1]
        assert "'9.0' as FTX_VRSN" in sql

    # Verify the FTX view includes the TAF file date.
    def test_includes_file_date(self, make_ftx):
        ftx, runner = make_ftx
        ftx.create(runner)
        sql = runner.append.call_args[0][1]
        assert "'2024-03-01' as FTX_FIL_DT" in sql

    # Verify payment date boundary constants (1600, 1599, 1960) are present.
    def test_pmt_date_boundary_logic(self, make_ftx):
        ftx, runner = make_ftx
        ftx.create(runner)
        sql = runner.append.call_args[0][1]
        assert "to_date('1600-01-01')" in sql
        assert "to_date('1599-12-31')" in sql
        assert "to_date('1960-01-01')" in sql

    # Verify SSN is trimmed and left-padded to 9 digits with zeros.
    def test_ssn_lpadded_to_9(self, make_ftx):
        ftx, runner = make_ftx
        ftx.create(runner)
        sql = runner.append.call_args[0][1]
        assert "lpad(trim(SSN_NUM),9,'0')" in sql

    # Verify ADJSTMT_IND_CLEAN subquery references COMBINED_FTX.
    def test_adjstmt_ind_clean_subquery(self, make_ftx):
        ftx, runner = make_ftx
        ftx.create(runner)
        sql = runner.append.call_args[0][1]
        assert "ADJSTMT_IND_CLEAN" in sql
        assert "COMBINED_FTX" in sql

    # Verify the FTX view includes REC_ADD_TS and REC_UPDT_TS.
    def test_includes_record_timestamps(self, make_ftx):
        ftx, runner = make_ftx
        ftx.create(runner)
        sql = runner.append.call_args[0][1]
        assert "REC_ADD_TS" in sql
        assert "REC_UPDT_TS" in sql

    # Verify create() appends with "FTX" as the key.
    def test_appends_with_ftx_key(self, make_ftx):
        ftx, runner = make_ftx
        ftx.create(runner)
        assert runner.append.call_args[0][0] == "FTX"


# =============================  build  =====================================
class TestBuild:

    # Verify stats-only mode skips the INSERT statement entirely.
    def test_stats_only_skips_insert(self, make_ftx):
        ftx, runner = make_ftx
        runner.run_stats_only = True
        ftx.build(runner)
        runner.append.assert_not_called()

    # Verify stats-only mode logs a "Run Stats Only" message.
    def test_stats_only_logs_skip_message(self, make_ftx):
        ftx, runner = make_ftx
        runner.run_stats_only = True
        ftx.build(runner)
        runner.logger.info.assert_called_once()
        assert "Run Stats Only" in runner.logger.info.call_args[0][0]

    # Verify a normal run INSERTs into the TAF_FTX table.
    def test_normal_run_inserts_into_taf_ftx(self, make_ftx):
        ftx, runner = make_ftx
        ftx.build(runner)
        sql = runner.append.call_args[0][1]
        assert "INSERT INTO prod_schema.TAF_FTX" in sql

    # Verify the INSERT selects FROM the FTX temporary view.
    def test_normal_run_selects_from_ftx_view(self, make_ftx):
        ftx, runner = make_ftx
        ftx.build(runner)
        sql = runner.append.call_args[0][1]
        assert "FROM FTX" in sql

    # Verify the target schema is parameterized via runner.DA_SCHEMA.
    def test_da_schema_parameterized(self, make_ftx):
        ftx, runner = make_ftx
        runner.DA_SCHEMA = "custom_schema"
        ftx.build(runner)
        sql = runner.append.call_args[0][1]
        assert "custom_schema.TAF_FTX" in sql

    # Verify build() appends with "FTX" as the key.
    def test_append_key_is_class_name(self, make_ftx):
        ftx, runner = make_ftx
        ftx.build(runner)
        assert runner.append.call_args[0][0] == "FTX"

    # Verify build() calls runner.append exactly once.
    def test_normal_run_calls_append_once(self, make_ftx):
        ftx, runner = make_ftx
        ftx.build(runner)
        assert runner.append.call_count == 1
