"""
Unit tests for taf.IP.IP_Metadata.

Covers:
- Segment column definitions
- selectDataElements
- Header, line, and diagnosis column renames
- finalFormatter
- dates_of_service
- plan_id_num
"""

import pytest


@pytest.fixture
def meta():
    """Return the IP_Metadata class for testing."""
    from taf.IP.IP_Metadata import IP_Metadata
    return IP_Metadata


# ===========================================================================
# Segment column definitions
# ===========================================================================

EXPECTED_SEGMENTS = [
    "CIP00001",
    "CIP00002",
    "CIP00003",
    "CIP00004",
]


# Test class: Checks IP segment and column metadata.
class TestColumns:

    # Test case: Verify all expected IP segments are defined.
    def test_expected_segments_exist(self, meta):
        assert set(meta.columns.keys()) == set(EXPECTED_SEGMENTS)

    # Test case: Verify each segment has column definitions.
    @pytest.mark.parametrize("segment", EXPECTED_SEGMENTS)
    def test_each_segment_has_columns(self, meta, segment):
        assert len(meta.columns[segment]) > 0

    # Test case: Verify each segment includes TMSIS_RUN_ID.
    @pytest.mark.parametrize("segment", EXPECTED_SEGMENTS)
    def test_each_segment_has_tmsis_run_id(self, meta, segment):
        assert "TMSIS_RUN_ID" in meta.columns[segment]


# ===========================================================================
# selectDataElements
# ===========================================================================

# Test class: Checks metadata-driven column selection and SQL expressions.
class TestSelectDataElements:

    # Test case: Verify the expected behavior for 'returns string'.
    def test_returns_string(self, meta):
        result = meta.selectDataElements("CIP00002", "a")
        assert isinstance(result, str)

    # Test case: Verify the expected behavior for 'output contains expected separator'.
    def test_output_contains_expected_separator(self, meta):
        result = meta.selectDataElements("CIP00002", "a")
        assert "\n\t\t\t," in result

    # Test case: Verify ordinary columns use the supplied table alias.
    def test_plain_column_uses_table_alias(self, meta):
        result = meta.selectDataElements("CIP00002", "a")
        assert "a.TMSIS_RUN_ID" in result

    # Test case: Verify the expected behavior for 'alias is parameterized'.
    def test_alias_is_parameterized(self, meta):
        result = meta.selectDataElements("CIP00002", "x")
        assert "x.TMSIS_RUN_ID" in result

    # Test case: Verify configured columns use uppercase conversion.
    def test_upper_column_is_transformed(self, meta):
        result = meta.selectDataElements("CIP00002", "a")
        assert "upper(a.SUBMTG_STATE_CD)" in result

    # Test case: Verify header columns use their expected output names.
    def test_header_columns_are_renamed(self, meta):
        result = meta.selectDataElements("CIP00002", "a")
        assert "srvc_fac_lctn_adr_line_1" in result.lower()

    # Test case: Verify line columns use their expected output names.
    def test_line_columns_are_renamed(self, meta):
        result = meta.selectDataElements("CIP00003", "a")
        assert "submtg_state_cd_line" in result.lower()

    # Test case: Verify diagnosis columns use their expected output names.
    def test_diagnosis_columns_are_renamed(self, meta):
        result = meta.selectDataElements("CIP00004", "a")
        assert "dgns_poa_ind" in result.lower()

    # Test case: Verify every supported segment produces output.
    @pytest.mark.parametrize("segment", EXPECTED_SEGMENTS)
    def test_all_segments_produce_output(self, meta, segment):
        result = meta.selectDataElements(segment, "a")
        assert result.strip()

    # Test case: Verify metadata definitions remain unchanged.
    def test_does_not_mutate_column_definitions(self, meta):
        original = meta.columns["CIP00002"].copy()

        meta.selectDataElements("CIP00002", "a")

        assert meta.columns["CIP00002"] == original

    # Test case: Verify an invalid segment raises an error.
    def test_invalid_segment_raises_error(self, meta):
        with pytest.raises(AttributeError):
            meta.selectDataElements("INVALID_SEGMENT", "a")


# ===========================================================================
# finalFormatter
# ===========================================================================

# Test class: Checks formatting and ordering of column expressions.
class TestFinalFormatter:

    # Test case: Verify the expected behavior for 'returns string'.
    def test_returns_string(self, meta):
        result = meta.finalFormatter(["DA_RUN_ID", "IP_VRSN"])
        assert isinstance(result, str)

    # Test case: Verify formatting preserves column order.
    def test_preserves_column_order(self, meta):
        result = meta.finalFormatter(["DA_RUN_ID", "IP_VRSN"])
        assert result.split("\n\t\t\t,") == ["DA_RUN_ID", "IP_VRSN"]

    # Test case: Verify the expected behavior for 'uses expected separator'.
    def test_uses_expected_separator(self, meta):
        result = meta.finalFormatter(["DA_RUN_ID", "IP_VRSN"])
        assert "\n\t\t\t," in result

    # Test case: Verify an empty input list returns an empty string.
    def test_empty_list_returns_empty_string(self, meta):
        assert meta.finalFormatter([]) == ""

    # Test case: Verify the helper does not modify its input.
    def test_does_not_mutate_input(self, meta):
        columns = ["DA_RUN_ID", "IP_VRSN"]
        original = columns.copy()

        meta.finalFormatter(columns)

        assert columns == original


# ===========================================================================
# dates_of_service
# ===========================================================================

# Test class: Checks date-of-service SQL expressions.
class TestDatesOfService:

    # Test case: Verify the expected behavior for 'returns string'.
    def test_returns_string(self, meta):
        result = meta.dates_of_service("DSCHRG_DT", "a")
        assert isinstance(result, str)

    # Test case: Verify the expected behavior for 'uses table alias and column'.
    def test_uses_table_alias_and_column(self, meta):
        result = meta.dates_of_service("DSCHRG_DT", "a")
        assert "a.DSCHRG_DT" in result

    # Test case: Verify the service end date expression is generated.
    def test_derives_service_end_date(self, meta):
        result = meta.dates_of_service("DSCHRG_DT", "a")
        assert "SRVC_ENDG_DT_DRVD_H" in result

    # Test case: Verify the service end date code expression is generated.
    def test_derives_service_end_date_code(self, meta):
        result = meta.dates_of_service("DSCHRG_DT", "a")
        assert "SRVC_ENDG_DT_CD_H" in result

    # Test case: Verify the expected behavior for 'uses default date'.
    def test_uses_default_date(self, meta):
        result = meta.dates_of_service("DSCHRG_DT", "a")
        assert "'1960-01-01'" in result

    # Test case: Verify the generated expression checks for null values.
    def test_uses_null_check(self, meta):
        result = meta.dates_of_service("DSCHRG_DT", "a")
        assert "is not null" in result


# ===========================================================================
# plan_id_num
# ===========================================================================

# Test class: Checks generation of plan ID expressions.
class TestPlanIdNum:

    # Test case: Verify the expected SQL expression is returned.
    def test_returns_expected_expression(self, meta):
        result = meta.plan_id_num("PLAN_ID_NUM", "a")
        assert result == "upper(a.PLAN_ID_NUM) as mc_plan_id"

    # Test case: Verify a custom table alias is used.
    def test_uses_custom_alias(self, meta):
        result = meta.plan_id_num("PLAN_ID_NUM", "x")
        assert result == "upper(x.PLAN_ID_NUM) as mc_plan_id"

    # Test case: Verify a custom column name is used.
    def test_uses_custom_column_name(self, meta):
        result = meta.plan_id_num("CUSTOM_PLAN_COL", "a")
        assert result == "upper(a.CUSTOM_PLAN_COL) as mc_plan_id"
