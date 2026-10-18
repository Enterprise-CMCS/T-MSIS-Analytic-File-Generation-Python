"""Unit tests for taf.IP.IP."""

from unittest.mock import MagicMock, patch

import pytest

from taf.IP.IP import IP


@pytest.fixture
def ip_instance():
    """Create an IP instance without initializing the TAF base class."""
    instance = IP.__new__(IP)
    instance.runner = MagicMock()
    instance.st_fil_type = "IP"
    instance.runner.get_combined_list.return_value = "'22123','22124'"
    return instance


class TestIPInitialization:
    @patch("taf.IP.IP.TAF.__init__", return_value=None)
    def test_init_sets_file_type(self, mock_taf_init):
        runner = MagicMock()

        instance = IP(runner)

        mock_taf_init.assert_called_once_with(runner)
        assert instance.st_fil_type == "IP"


class TestAWSExtractLine:
    @patch("taf.IP.IP.TAF_Closure.var_set_taxo", return_value="MOCK_TAXONOMY_SQL")
    @patch("taf.IP.IP.IP_Metadata.selectDataElements", return_value="MOCK_SELECTED_COLUMNS")
    def test_appends_five_sql_statements(
        self, mock_select, mock_taxonomy, ip_instance
    ):
        ip_instance.AWS_Extract_Line(
            "TMSIS_SCHEMA",
            "DA_SCHEMA",
            "TMP",
            "IP",
            "CIP00003",
            "TMSIS_LINE_SEGMENT",
            3,
        )

        assert ip_instance.runner.append.call_count == 5

    @patch("taf.IP.IP.TAF_Closure.var_set_taxo", return_value="MOCK_TAXONOMY_SQL")
    @patch("taf.IP.IP.IP_Metadata.selectDataElements", return_value="MOCK_SELECTED_COLUMNS")
    def test_uses_expected_segment_and_schema(
        self, mock_select, mock_taxonomy, ip_instance
    ):
        ip_instance.AWS_Extract_Line(
            "RAW_SCHEMA",
            "DA_SCHEMA",
            "TMP",
            "IP",
            "CIP00003",
            "TMSIS_LINE_SEGMENT",
            2,
        )

        statements = [
            call.args[1]
            for call in ip_instance.runner.append.call_args_list
        ]

        assert "RAW_SCHEMA.TMSIS_LINE_SEGMENT" in statements[0]
        assert "MOCK_SELECTED_COLUMNS" in statements[0]
        mock_select.assert_called_once_with("CIP00003", "a")

    @patch("taf.IP.IP.TAF_Closure.var_set_taxo", return_value="MOCK_TAXONOMY_SQL")
    @patch("taf.IP.IP.IP_Metadata.selectDataElements", return_value="MOCK_SELECTED_COLUMNS")
    def test_includes_requested_diagnosis_columns(
        self, mock_select, mock_taxonomy, ip_instance
    ):
        ip_instance.AWS_Extract_Line(
            "RAW_SCHEMA",
            "DA_SCHEMA",
            "TMP",
            "IP",
            "CIP00003",
            "TMSIS_LINE_SEGMENT",
            3,
        )

        statements = [
            call.args[1]
            for call in ip_instance.runner.append.call_args_list
        ]
        header_sql = statements[4]

        assert "dx.dgns_1_cd" in header_sql
        assert "dx.dgns_2_cd" in header_sql
        assert "dx.dgns_3_cd" in header_sql
        assert "dx.dgns_4_cd" not in header_sql

    @patch("taf.IP.IP.TAF_Closure.var_set_taxo", return_value="MOCK_TAXONOMY_SQL")
    @patch("taf.IP.IP.IP_Metadata.selectDataElements", return_value="MOCK_SELECTED_COLUMNS")
    def test_appends_statements_under_ip_file_type(
        self, mock_select, mock_taxonomy, ip_instance
    ):
        ip_instance.AWS_Extract_Line(
            "RAW_SCHEMA",
            "DA_SCHEMA",
            "TMP",
            "IP",
            "CIP00003",
            "TMSIS_LINE_SEGMENT",
            1,
        )

        for call in ip_instance.runner.append.call_args_list:
            assert call.args[0] == "IP"


class TestSelectDx:
    @patch("taf.IP.IP.IP_Metadata.selectDataElements", return_value="MOCK_DX_COLUMNS")
    def test_appends_two_sql_statements(self, mock_select, ip_instance):
        ip_instance.select_dx(
            "RAW_SCHEMA",
            "CIP00004",
            "TMSIS_DX_SEGMENT",
            "IP",
            "HEADER_INPUT",
            3,
        )

        assert ip_instance.runner.append.call_count == 2

    @patch("taf.IP.IP.IP_Metadata.selectDataElements", return_value="MOCK_DX_COLUMNS")
    def test_uses_expected_schema_segment_and_header(
        self, mock_select, ip_instance
    ):
        ip_instance.select_dx(
            "RAW_SCHEMA",
            "CIP00004",
            "TMSIS_DX_SEGMENT",
            "IP",
            "HEADER_INPUT",
            2,
        )

        statements = [
            call.args[1]
            for call in ip_instance.runner.append.call_args_list
        ]

        assert "RAW_SCHEMA.TMSIS_DX_SEGMENT" in statements[0]
        assert "HEADER_INPUT" in statements[0]
        assert "MOCK_DX_COLUMNS" in statements[0]
        mock_select.assert_called_once_with("CIP00004", "a")

    @patch("taf.IP.IP.IP_Metadata.selectDataElements", return_value="MOCK_DX_COLUMNS")
    def test_generates_requested_diagnosis_fields(
        self, mock_select, ip_instance
    ):
        ip_instance.select_dx(
            "RAW_SCHEMA",
            "CIP00004",
            "TMSIS_DX_SEGMENT",
            "IP",
            "HEADER_INPUT",
            3,
        )

        wide_sql = ip_instance.runner.append.call_args_list[1].args[1]

        assert "as dgns_1_cd" in wide_sql
        assert "as dgns_2_cd" in wide_sql
        assert "as dgns_3_cd" in wide_sql
        assert "as dgns_4_cd" not in wide_sql

    @patch("taf.IP.IP.IP_Metadata.selectDataElements", return_value="MOCK_DX_COLUMNS")
    def test_zero_diagnosis_fields(self, mock_select, ip_instance):
        ip_instance.select_dx(
            "RAW_SCHEMA",
            "CIP00004",
            "TMSIS_DX_SEGMENT",
            "IP",
            "HEADER_INPUT",
            0,
        )

        wide_sql = ip_instance.runner.append.call_args_list[1].args[1]

        assert "as dgns_1_cd" not in wide_sql

    @patch("taf.IP.IP.IP_Metadata.selectDataElements", return_value="MOCK_DX_COLUMNS")
    def test_appends_under_requested_file_type(self, mock_select, ip_instance):
        ip_instance.select_dx(
            "RAW_SCHEMA",
            "CIP00004",
            "TMSIS_DX_SEGMENT",
            "OTHR_TOC",
            "HEADER_INPUT",
            1,
        )

        for call in ip_instance.runner.append.call_args_list:
            assert call.args[0] == "OTHR_TOC"
