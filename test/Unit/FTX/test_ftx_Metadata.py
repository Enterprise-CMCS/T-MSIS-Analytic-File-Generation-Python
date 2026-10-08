"""
Unit tests for taf.FTX.FTX_Metadata — column definitions, cleansers,
renames, and the selectDataElements / finalFormatter methods.
"""



# ===========================================================================
# Constants & Fixtures
# ===========================================================================
ALL_SEGMENT_IDS = [
    "FTX00002", "FTX00003", "FTX00004", "FTX00005", "FTX00006",
    "FTX00007", "FTX00008", "FTX00009", "FTX00095",
]


@pytest.fixture
def meta():
    from taf.FTX.FTX_Metadata import FTX_Metadata
    return FTX_Metadata


# ========================  columns dict  ====================================
class TestColumns:

    # Verify the columns dict contains exactly the nine expected segment IDs.
    def test_exactly_nine_segments(self, meta):
        assert set(meta.columns.keys()) == set(ALL_SEGMENT_IDS)  # noqa: SCPAP001

    # Verify each segment has at least one column defined.
    @pytest.mark.parametrize("seg", ALL_SEGMENT_IDS)
    def test_segment_is_non_empty(self, meta, seg):
        assert len(meta.columns[seg]) > 0  # noqa: SCPAP001

    # Verify TMSIS_RUN_ID is always the first column in every segment.
    @pytest.mark.parametrize("seg", ALL_SEGMENT_IDS)
    def test_tmsis_run_id_is_first(self, meta, seg):
        assert meta.columns[seg][0] == "TMSIS_RUN_ID"  # noqa: SCPAP001

    # Verify every segment includes the SUBMTG_STATE_CD column.
    @pytest.mark.parametrize("seg", ALL_SEGMENT_IDS)
    def test_submtg_state_cd_present(self, meta, seg):
        assert "SUBMTG_STATE_CD" in meta.columns[seg]  # noqa: SCPAP001

    # Verify every segment includes the ADJSTMT_IND column.
    @pytest.mark.parametrize("seg", ALL_SEGMENT_IDS)
    def test_adjstmt_ind_present(self, meta, seg):
        assert "ADJSTMT_IND" in meta.columns[seg]  # noqa: SCPAP001

    # Verify every segment includes the ORGNL_CLM_NUM column.
    @pytest.mark.parametrize("seg", ALL_SEGMENT_IDS)
    def test_orgnl_clm_num_present(self, meta, seg):
        assert "ORGNL_CLM_NUM" in meta.columns[seg]  # noqa: SCPAP001

    # Verify no segment has duplicate column names.
    @pytest.mark.parametrize("seg", ALL_SEGMENT_IDS)
    def test_no_duplicate_columns(self, meta, seg):
        cols = meta.columns[seg]  # noqa: SCPAP001
        assert len(cols) == len(set(cols)), f"Duplicates in {seg}"

    # Verify the FTX00002 (capitation) segment has exactly 43 columns, spot check.
    def test_ftx00002_has_43_columns(self, meta):
        assert len(meta.columns["FTX00002"]) == 43  # noqa: SCPAP001


# ========================  cleanser dict  ===================================
class TestCleanser:

    # Verify the cleanser dict contains exactly the nine expected segment IDs.
    def test_exactly_nine_segments(self, meta):
        assert set(meta.cleanser.keys()) == set(ALL_SEGMENT_IDS)

    # Verify every cleanser value is a callable function.
    @pytest.mark.parametrize("seg", ALL_SEGMENT_IDS)
    def test_all_values_are_callable(self, meta, seg):
        for key, func in meta.cleanser[seg].items():
            assert callable(func), f"{key} in {seg} not callable"

    # Verify every segment has a cleanser for ADJSTMT_IND.
    @pytest.mark.parametrize("seg", ALL_SEGMENT_IDS)
    def test_adjstmt_ind_cleanser_present(self, meta, seg):
        assert "ADJSTMT_IND" in meta.cleanser[seg]

    # Verify every segment has a cleanser for ORGNL_CLM_NUM.
    @pytest.mark.parametrize("seg", ALL_SEGMENT_IDS)
    def test_orgnl_clm_num_cleanser_present(self, meta, seg):
        assert "ORGNL_CLM_NUM" in meta.cleanser[seg]

    # Verify every segment has a cleanser for ADJSTMT_CLM_NUM.
    @pytest.mark.parametrize("seg", ALL_SEGMENT_IDS)
    def test_adjstmt_clm_num_cleanser_present(self, meta, seg):
        assert "ADJSTMT_CLM_NUM" in meta.cleanser[seg]

    # Verify ADJDCTN_DT exists in every cleanser but is absent from columns (dead code).
    @pytest.mark.parametrize("seg", ALL_SEGMENT_IDS)
    def test_adjdctn_dt_cleanser_is_dead_code(self, meta, seg):
        """ADJDCTN_DT in every cleanser but NOT in any columns list — never triggered."""
        assert "ADJDCTN_DT" in meta.cleanser[seg]
        assert "ADJDCTN_DT" not in meta.columns[seg]  # noqa: SCPAP001


# ========================  renames dict  ====================================
class TestRenames:

    # Verify the renames dict contains exactly the nine expected segment IDs.
    def test_exactly_nine_segments(self, meta):
        assert set(meta.renames.keys()) == set(ALL_SEGMENT_IDS)

    # Verify PAYERID is renamed to PYR_ID in every segment.
    @pytest.mark.parametrize("seg", ALL_SEGMENT_IDS)
    def test_payerid_renamed_to_pyr_id(self, meta, seg):
        assert meta.renames[seg].get("PAYERID") == "PYR_ID"

    # Verify PAYERID_TYPE is renamed to PYR_ID_TYPE_CD in every segment.
    @pytest.mark.parametrize("seg", ALL_SEGMENT_IDS)
    def test_payerid_type_renamed(self, meta, seg):
        assert meta.renames[seg].get("PAYERID_TYPE") == "PYR_ID_TYPE_CD"

    # Verify EXPNDTR_AUTHRTY_TYPE is renamed to EXPNDTR_AUTHRTY_TYPE_CD.
    @pytest.mark.parametrize("seg", ALL_SEGMENT_IDS)
    def test_expndtr_authrty_type_renamed(self, meta, seg):
        assert meta.renames[seg].get("EXPNDTR_AUTHRTY_TYPE") == "EXPNDTR_AUTHRTY_TYPE_CD"

    # Verify FED_REIMBRSMT_CTGRY_CD is renamed to FED_RIMBRSMT_CTGRY.
    @pytest.mark.parametrize("seg", ALL_SEGMENT_IDS)
    def test_fed_reimbrsmt_renamed(self, meta, seg):
        assert meta.renames[seg].get("FED_REIMBRSMT_CTGRY_CD") == "FED_RIMBRSMT_CTGRY"

    # Verify MBESCBES_FORM_GRP is renamed to MBESCBES_FRM_GRP.
    @pytest.mark.parametrize("seg", ALL_SEGMENT_IDS)
    def test_mbescbes_form_grp_renamed(self, meta, seg):
        assert meta.renames[seg].get("MBESCBES_FORM_GRP") == "MBESCBES_FRM_GRP"

    # Verify MBESCBES_FORM is renamed to MBESCBES_FRM.
    @pytest.mark.parametrize("seg", ALL_SEGMENT_IDS)
    def test_mbescbes_form_renamed(self, meta, seg):
        assert meta.renames[seg].get("MBESCBES_FORM") == "MBESCBES_FRM"

    # Verify MBESCBES_SRVC_CTGRY_CD is renamed to MBESCBES_SRVC_CTGRY.
    @pytest.mark.parametrize("seg", ALL_SEGMENT_IDS)
    def test_mbescbes_srvc_ctgry_renamed(self, meta, seg):
        assert meta.renames[seg].get("MBESCBES_SRVC_CTGRY_CD") == "MBESCBES_SRVC_CTGRY"


# ========================  upper set  =======================================
class TestUpper:

    # Verify the upper attribute is a set type.
    def test_is_a_set(self, meta):
        assert isinstance(meta.upper, set)

    # Verify SUBMTG_STATE_CD is in the upper-cased columns set.
    def test_submtg_state_cd_included(self, meta):
        assert "SUBMTG_STATE_CD" in meta.upper

    # Verify MSIS_IDENT_NUM is in the upper-cased columns set.
    def test_msis_ident_num_included(self, meta):
        assert "MSIS_IDENT_NUM" in meta.upper

    # Verify FUNDNG_CD is in the upper-cased columns set.
    def test_fundng_cd_included(self, meta):
        assert "FUNDNG_CD" in meta.upper

    # Verify DA_RUN_ID is excluded from the upper-cased columns set.
    def test_da_run_id_not_included(self, meta):
        assert "DA_RUN_ID" not in meta.upper

    # Verify TMSIS_RUN_ID is excluded from the upper-cased columns set.
    def test_tmsis_run_id_not_included(self, meta):
        assert "TMSIS_RUN_ID" not in meta.upper


# ========================  ftx_cols  ========================================
class TestFtxCols:

    # Verify the final output column list has exactly 50 entries.
    def test_has_50_columns(self, meta):
        assert len(meta.ftx_cols) == 50

    # Verify DA_RUN_ID is the first column in the final output list.
    def test_starts_with_da_run_id(self, meta):
        assert meta.ftx_cols[0] == "DA_RUN_ID"

    # Verify REC_UPDT_TS is the last column in the final output list.
    def test_ends_with_rec_updt_ts(self, meta):
        assert meta.ftx_cols[-1] == "REC_UPDT_TS"

    # Verify there are no duplicate column names in the final output list.
    def test_no_duplicates(self, meta):
        assert len(meta.ftx_cols) == len(set(meta.ftx_cols))

    # Verify all essential output columns (IDs, dates, amounts, timestamps) are present.
    def test_contains_key_output_columns(self, meta):
        required = [
            "DA_RUN_ID", "FTX_VRSN", "FTX_FIL_DT",
            "TMSIS_RUN_ID", "SUBMTG_STATE_CD",
            "ORGNL_CLM_NUM", "ADJSTMT_CLM_NUM", "ADJSTMT_IND",
            "PMT_OR_RCPMT_DT", "PMT_OR_RCPMT_AMT",
            "REC_ADD_TS", "REC_UPDT_TS",
        ]
        for col in required:
            assert col in meta.ftx_cols, f"Missing {col}"

    # Verify ftx_cols uses renamed names (e.g. PYR_ID) not raw TMSIS names (e.g. PAYERID).
    def test_uses_post_rename_column_names(self, meta):
        """ftx_cols must use the renamed names, not raw TMSIS names."""
        assert "PYR_ID" in meta.ftx_cols
        assert "PAYERID" not in meta.ftx_cols
        assert "FED_RIMBRSMT_CTGRY" in meta.ftx_cols
        assert "MBESCBES_FRM" in meta.ftx_cols


# ===================  selectDataElements  ===================================
class TestSelectDataElements:

    # Verify selectDataElements returns a string.
    def test_returns_string(self, meta):
        result = meta.selectDataElements("FTX00002", "a")
        assert isinstance(result, str)

    # Verify the output contains commas separating the column expressions.
    def test_comma_separated_output(self, meta):
        result = meta.selectDataElements("FTX00002", "a")
        assert "," in result

    # Verify the number of output items matches the segment's column count.
    @pytest.mark.parametrize("seg", ALL_SEGMENT_IDS)
    def test_item_count_matches_column_count(self, meta, seg):
        result = meta.selectDataElements(seg, "a")
        items = [i.strip() for i in result.split("\n\t\t\t,")]
        assert len(items) == len(meta.columns[seg])  # noqa: SCPAP001

    # Verify plain columns are prefixed with the table alias (e.g. a.TMSIS_RUN_ID).
    def test_plain_column_gets_alias_prefix(self, meta):
        result = meta.selectDataElements("FTX00002", "a")
        assert "a.TMSIS_RUN_ID" in result

    # Verify changing the alias parameter changes the prefix in the output.
    def test_alias_is_parameterized(self, meta):
        result = meta.selectDataElements("FTX00002", "x")
        assert "x.TMSIS_RUN_ID" in result

    # Verify columns in the upper set are wrapped with upper() in the output.
    def test_upper_column_gets_upper_wrapper(self, meta):
        result = meta.selectDataElements("FTX00002", "a")
        assert "upper(a.SUBMTG_STATE_CD)" in result

    # Verify renamed columns appear under their new name (e.g. PYR_ID not PAYERID).
    def test_renamed_column_uses_new_alias(self, meta):
        result = meta.selectDataElements("FTX00002", "a")
        assert "pyr_id" in result.lower()

    # Verify cleansed columns are transformed, not output as plain alias.column.
    def test_cleansed_column_is_not_plain(self, meta):
        result = meta.selectDataElements("FTX00002", "a")
        items = [i.strip() for i in result.split(",")]
        assert "a.ADJSTMT_IND" not in items

    # Verify every segment produces a non-empty output string.
    @pytest.mark.parametrize("seg", ALL_SEGMENT_IDS)
    def test_all_segments_produce_nonempty_output(self, meta, seg):
        result = meta.selectDataElements(seg, "a")
        assert len(result) > 0

    # Verify selectDataElements does not alter the underlying columns dict.
    def test_does_not_mutate_columns_dict(self, meta):
        original_len = len(meta.columns["FTX00002"])  # noqa: SCPAP001
        meta.selectDataElements("FTX00002", "a")
        assert len(meta.columns["FTX00002"]) == original_len  # noqa: SCPAP001

    # Verify an invalid segment ID raises a KeyError or AttributeError.
    def test_invalid_segment_raises_error(self, meta):
        with pytest.raises((KeyError, AttributeError)):
            meta.selectDataElements("INVALID_SEGMENT", "a")


# =====================  finalFormatter  =====================================
class TestFinalFormatter:

    # Verify finalFormatter returns a string.
    def test_returns_string(self, meta):
        result = meta.finalFormatter(["DA_RUN_ID"])
        assert isinstance(result, str)

    # Verify non-upper columns pass through unchanged.
    def test_non_upper_column_unchanged(self, meta):
        result = meta.finalFormatter(["DA_RUN_ID"])
        assert result.strip() == "DA_RUN_ID"

    # Verify columns in the upper set are wrapped with upper().
    def test_upper_column_wrapped(self, meta):
        result = meta.finalFormatter(["SUBMTG_STATE_CD"])
        assert "upper(SUBMTG_STATE_CD)" in result

    # Verify mixed columns retain their order with correct upper() wrapping.
    def test_mixed_columns_correct_order(self, meta):
        result = meta.finalFormatter(["DA_RUN_ID", "SUBMTG_STATE_CD", "FTX_VRSN"])
        items = [i.strip() for i in result.split(",")]
        assert items[0] == "DA_RUN_ID"
        assert items[1] == "upper(SUBMTG_STATE_CD)"
        assert items[2] == "FTX_VRSN"

    # Verify an empty input list returns an empty string.
    def test_empty_list_returns_empty(self, meta):
        result = meta.finalFormatter([])
        assert result == ""

    # Verify finalFormatter does not mutate the input list.
    def test_does_not_mutate_input(self, meta):
        original = ["DA_RUN_ID", "SUBMTG_STATE_CD"]
        copy = original.copy()
        meta.finalFormatter(original)
        assert original == copy

    # Verify formatting all 50 ftx_cols produces exactly 50 comma-separated items.
    def test_full_ftx_cols_item_count(self, meta):
        result = meta.finalFormatter(meta.ftx_cols)
        items = [i.strip() for i in result.split(",")]
        assert len(items) == 50

    # Verify items are joined with a newline-tab-comma separator.
    def test_uses_correct_separator(self, meta):
        result = meta.finalFormatter(["DA_RUN_ID", "FTX_VRSN"])
        assert "\n\t\t\t," in result
