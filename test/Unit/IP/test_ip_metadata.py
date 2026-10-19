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


# TEST PURPOSE:
# Verify that IP metadata defines the expected segments and required columns.
# These checks help catch missing segments or incomplete column definitions.
class TestColumns:

    # TEST CASE:
    # Check that all expected IP segments are defined.
    def test_expected_segments_exist(self, meta):
        assert set(meta.columns.keys()) == set(EXPECTED_SEGMENTS)

    # TEST CASE:
    # Check that each supported segment has column definitions.
    @pytest.mark.parametrize("segment", EXPECTED_SEGMENTS)
    def test_each_segment_has_columns(self, meta, segment):
        assert len(meta.columns[segment]) > 0

    # TEST CASE:
    # Check that each segment includes the required TMSIS_RUN_ID column.
    @pytest.mark.parametrize("segment", EXPECTED_SEGMENTS)
    def test_each_segment_has_tmsis_run_id(self, meta, segment):
        assert "TMSIS_RUN_ID" in meta.columns[segment]


# ===========================================================================
# selectDataElements
# ===========================================================================

# TEST PURPOSE:
# Verify that metadata is converted into the expected SQL column expressions.
# These tests cover aliases, transformations, renaming, supported segments,
# and error handling for invalid segment identifiers.
class TestSelectDataElements:

    # TEST CASE:
    # Check that the helper returns the expected type of result.
    def test_returns_string(self, meta):
        result = meta.selectDataElements("CIP00002", "a")
        assert isinstance(result, str)

    # TEST CASE:
    # Check that generated column expressions use the expected separator.
    def test_output_contains_expected_separator(self, meta):
        result = meta.selectDataElements("CIP00002", "a")
        assert "\n\t\t\t," in result

    # TEST CASE:
    # Check that ordinary columns are qualified with the supplied table alias.
    def test_plain_column_uses_table_alias(self, meta):
        result = meta.selectDataElements("CIP00002", "a")
        assert "a.TMSIS_RUN_ID" in result

    # TEST CASE:
    # Check that the supplied table alias is used in the generated expressions.
    def test_alias_is_parameterized(self, meta):
        result = meta.selectDataElements("CIP00002", "x")
        assert "x.TMSIS_RUN_ID" in result

    # TEST CASE:
    # Check that configured columns use the expected uppercase transformation.
    def test_upper_column_is_transformed(self, meta):
        result = meta.selectDataElements("CIP00002", "a")
        assert "upper(a.SUBMTG_STATE_CD)" in result

    # TEST CASE:
    # Check that header columns receive their expected output names.
    def test_header_columns_are_renamed(self, meta):
        result = meta.selectDataElements("CIP00002", "a")
        assert "srvc_fac_lctn_adr_line_1" in result.lower()

    # TEST CASE:
    # Check that line columns receive their expected output names.
    def test_line_columns_are_renamed(self, meta):
        result = meta.selectDataElements("CIP00003", "a")
        assert "submtg_state_cd_line" in result.lower()

    # TEST CASE:
    # Check that diagnosis columns receive their expected output names.
    def test_diagnosis_columns_are_renamed(self, meta):
        result = meta.selectDataElements("CIP00004", "a")
        assert "dgns_poa_ind" in result.lower()

    # TEST CASE:
    # Check that every supported segment produces column-selection output.
    @pytest.mark.parametrize("segment", EXPECTED_SEGMENTS)
    def test_all_segments_produce_output(self, meta, segment):
        result = meta.selectDataElements(segment, "a")
        assert result.strip()

    # TEST CASE:
    # Check that selecting columns does not modify the original metadata.
    def test_does_not_mutate_column_definitions(self, meta):
        original = meta.columns["CIP00002"].copy()

        meta.selectDataElements("CIP00002", "a")

        assert meta.columns["CIP00002"] == original

    # TEST CASE:
    # Check that an unsupported segment raises the expected error.
    def test_invalid_segment_raises_error(self, meta):
        with pytest.raises(AttributeError):
            meta.selectDataElements("INVALID_SEGMENT", "a")


# ===========================================================================
# finalFormatter
# ===========================================================================

# TEST PURPOSE:
# Verify that column expressions are formatted correctly for SQL generation.
# These checks cover the return type, separators, column order, empty input,
# and preservation of the original input list.
class TestFinalFormatter:

    # TEST CASE:
    # Check that the helper returns the expected type of result.
    def test_returns_string(self, meta):
        result = meta.finalFormatter(["DA_RUN_ID", "IP_VRSN"])
        assert isinstance(result, str)

    # TEST CASE:
    # Check that formatting preserves the original column order.
    def test_preserves_column_order(self, meta):
        result = meta.finalFormatter(["DA_RUN_ID", "IP_VRSN"])
        assert result.split("\n\t\t\t,") == ["DA_RUN_ID", "IP_VRSN"]

    # TEST CASE:
    # Check that the formatted output uses the expected separator.
    def test_uses_expected_separator(self, meta):
        result = meta.finalFormatter(["DA_RUN_ID", "IP_VRSN"])
        assert "\n\t\t\t," in result

    # TEST CASE:
    # Check that an empty input list produces an empty string.
    def test_empty_list_returns_empty_string(self, meta):
        assert meta.finalFormatter([]) == ""

    # TEST CASE:
    # Check that formatting leaves the original input unchanged.
    def test_does_not_mutate_input(self, meta):
        columns = ["DA_RUN_ID", "IP_VRSN"]
        original = columns.copy()

        meta.finalFormatter(columns)

        assert columns == original


# ===========================================================================
# dates_of_service
# ===========================================================================

# TEST PURPOSE:
# Verify that the date-of-service helper builds the expected SQL expressions.
# These tests check aliases, date derivation, default values, and null handling.
class TestDatesOfService:

    # TEST CASE:
    # Check that the helper returns the expected type of result.
    def test_returns_string(self, meta):
        result = meta.dates_of_service("DSCHRG_DT", "a")
        assert isinstance(result, str)

    # TEST CASE:
    # Check that the date expression uses the supplied alias and column.
    def test_uses_table_alias_and_column(self, meta):
        result = meta.dates_of_service("DSCHRG_DT", "a")
        assert "a.DSCHRG_DT" in result

    # TEST CASE:
    # Check that the service end date expression is generated correctly.
    def test_derives_service_end_date(self, meta):
        result = meta.dates_of_service("DSCHRG_DT", "a")
        assert "SRVC_ENDG_DT_DRVD_H" in result

    # TEST CASE:
    # Check that the service end date code expression is generated correctly.
    def test_derives_service_end_date_code(self, meta):
        result = meta.dates_of_service("DSCHRG_DT", "a")
        assert "SRVC_ENDG_DT_CD_H" in result

    # TEST CASE:
    # Check that the expected default date is used.
    def test_uses_default_date(self, meta):
        result = meta.dates_of_service("DSCHRG_DT", "a")
        assert "'1960-01-01'" in result

    # TEST CASE:
    # Check that the generated date expression handles null values.
    def test_uses_null_check(self, meta):
        result = meta.dates_of_service("DSCHRG_DT", "a")
        assert "is not null" in result


# ===========================================================================
# plan_id_num
# ===========================================================================

# TEST PURPOSE:
# Verify that the plan ID helper generates the expected SQL expression.
# These tests also check whether custom aliases and column names are respected.
class TestPlanIdNum:

    # TEST CASE:
    # Check that the plan ID helper returns the expected SQL expression.
    def test_returns_expected_expression(self, meta):
        result = meta.plan_id_num("PLAN_ID_NUM", "a")
        assert result == "upper(a.PLAN_ID_NUM) as mc_plan_id"

    # TEST CASE:
    # Check that the plan ID expression uses the requested table alias.
    def test_uses_custom_alias(self, meta):
        result = meta.plan_id_num("PLAN_ID_NUM", "x")
        assert result == "upper(x.PLAN_ID_NUM) as mc_plan_id"

    # TEST CASE:
    # Check that the plan ID expression uses the requested column name.
    def test_uses_custom_column_name(self, meta):
        result = meta.plan_id_num("CUSTOM_PLAN_COL", "a")
        assert result == "upper(a.CUSTOM_PLAN_COL) as mc_plan_id"
