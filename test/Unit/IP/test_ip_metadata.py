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


class TestColumns:

    def test_expected_segments_exist(self, meta):
        assert set(meta.columns.keys()) == set(EXPECTED_SEGMENTS)

    @pytest.mark.parametrize("segment", EXPECTED_SEGMENTS)
    def test_each_segment_has_columns(self, meta, segment):
        assert len(meta.columns[segment]) > 0

    @pytest.mark.parametrize("segment", EXPECTED_SEGMENTS)
    def test_each_segment_has_tmsis_run_id(self, meta, segment):
        assert "TMSIS_RUN_ID" in meta.columns[segment]


# ===========================================================================
# selectDataElements
# ===========================================================================

class TestSelectDataElements:

    def test_returns_string(self, meta):
        result = meta.selectDataElements("CIP00002", "a")
        assert isinstance(result, str)

    def test_output_contains_expected_separator(self, meta):
        result = meta.selectDataElements("CIP00002", "a")
        assert "\n\t\t\t," in result

    def test_plain_column_uses_table_alias(self, meta):
        result = meta.selectDataElements("CIP00002", "a")
        assert "a.TMSIS_RUN_ID" in result

    def test_alias_is_parameterized(self, meta):
        result = meta.selectDataElements("CIP00002", "x")
        assert "x.TMSIS_RUN_ID" in result

    def test_upper_column_is_transformed(self, meta):
        result = meta.selectDataElements("CIP00002", "a")
        assert "upper(a.SUBMTG_STATE_CD)" in result

    def test_header_columns_are_renamed(self, meta):
        result = meta.selectDataElements("CIP00002", "a")
        assert "srvc_fac_lctn_adr_line_1" in result.lower()

    def test_line_columns_are_renamed(self, meta):
        result = meta.selectDataElements("CIP00003", "a")
        assert "submtg_state_cd_line" in result.lower()

    def test_diagnosis_columns_are_renamed(self, meta):
        result = meta.selectDataElements("CIP00004", "a")
        assert "dgns_poa_ind" in result.lower()

    @pytest.mark.parametrize("segment", EXPECTED_SEGMENTS)
    def test_all_segments_produce_output(self, meta, segment):
        result = meta.selectDataElements(segment, "a")
        assert result.strip()

    def test_does_not_mutate_column_definitions(self, meta):
        original = meta.columns["CIP00002"].copy()

        meta.selectDataElements("CIP00002", "a")

        assert meta.columns["CIP00002"] == original

    def test_invalid_segment_raises_error(self, meta):
        with pytest.raises(AttributeError):
            meta.selectDataElements("INVALID_SEGMENT", "a")


# ===========================================================================
# finalFormatter
# ===========================================================================

class TestFinalFormatter:

    def test_returns_string(self, meta):
        result = meta.finalFormatter(["DA_RUN_ID", "IP_VRSN"])
        assert isinstance(result, str)

    def test_preserves_column_order(self, meta):
        result = meta.finalFormatter(["DA_RUN_ID", "IP_VRSN"])
        assert result.split("\n\t\t\t,") == ["DA_RUN_ID", "IP_VRSN"]

    def test_uses_expected_separator(self, meta):
        result = meta.finalFormatter(["DA_RUN_ID", "IP_VRSN"])
        assert "\n\t\t\t," in result

    def test_empty_list_returns_empty_string(self, meta):
        assert meta.finalFormatter([]) == ""

    def test_does_not_mutate_input(self, meta):
        columns = ["DA_RUN_ID", "IP_VRSN"]
        original = columns.copy()

        meta.finalFormatter(columns)

        assert columns == original


# ===========================================================================
# dates_of_service
# ===========================================================================

class TestDatesOfService:

    def test_returns_string(self, meta):
        result = meta.dates_of_service("DSCHRG_DT", "a")
        assert isinstance(result, str)

    def test_uses_table_alias_and_column(self, meta):
        result = meta.dates_of_service("DSCHRG_DT", "a")
        assert "a.DSCHRG_DT" in result

    def test_derives_service_end_date(self, meta):
        result = meta.dates_of_service("DSCHRG_DT", "a")
        assert "SRVC_ENDG_DT_DRVD_H" in result

    def test_derives_service_end_date_code(self, meta):
        result = meta.dates_of_service("DSCHRG_DT", "a")
        assert "SRVC_ENDG_DT_CD_H" in result

    def test_uses_default_date(self, meta):
        result = meta.dates_of_service("DSCHRG_DT", "a")
        assert "'1960-01-01'" in result

    def test_uses_null_check(self, meta):
        result = meta.dates_of_service("DSCHRG_DT", "a")
        assert "is not null" in result


# ===========================================================================
# plan_id_num
# ===========================================================================

class TestPlanIdNum:

    def test_returns_expected_expression(self, meta):
        result = meta.plan_id_num("PLAN_ID_NUM", "a")
        assert result == "upper(a.PLAN_ID_NUM) as mc_plan_id"

    def test_uses_custom_alias(self, meta):
        result = meta.plan_id_num("PLAN_ID_NUM", "x")
        assert result == "upper(x.PLAN_ID_NUM) as mc_plan_id"

    def test_uses_custom_column_name(self, meta):
        result = meta.plan_id_num("CUSTOM_PLAN_COL", "a")
        assert result == "upper(a.CUSTOM_PLAN_COL) as mc_plan_id"
