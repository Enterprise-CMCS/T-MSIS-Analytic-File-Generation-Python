"""Unit tests for taf.IP.IP_Runner."""

from unittest.mock import patch

from taf.IP.IP_Runner import IP_Runner


# Test case: Verify the runner invokes the expected IP build components.
def test_init_calls_expected_build_steps():
    """Verify IP_Runner.init orchestrates the expected IP build steps."""
    runner = IP_Runner.__new__(IP_Runner)
    runner.DA_SCHEMA = "DA_SCHEMA"

    with (
        patch("taf.IP.IP_Runner.TAF_Grouper") as mock_grouper_class,
        patch("taf.IP.IP_Runner.TAF_Claims") as mock_claims_class,
        patch("taf.IP.IP.IP") as mock_ip_class,
        patch("taf.IP.IPH.IPH") as mock_iph_class,
        patch("taf.IP.IPL.IPL") as mock_ipl_class,
        patch("taf.IP.IP_DX.IP_DX") as mock_ip_dx_class,
    ):
        runner.init()

    grouper = mock_grouper_class.return_value
    claims = mock_claims_class.return_value
    ip = mock_ip_class.return_value

    grouper.fetch_nppes.assert_called_once_with("IP")
    grouper.fetch_ccs.assert_called_once_with("IP")

    claims.AWS_Claims_Family_Table_Link.assert_called_once_with(
        "dc_prod_data_sources_catalog.tmsis",
        "CIP00002",
        "TMSIS_CLH_REC_IP",
        "IP",
        "DSCHRG_DT",
    )

    ip.select_dx.assert_called_once_with(
        "dc_prod_data_sources_catalog.tmsis",
        "CIP00004",
        "tmsis_clm_dx_ip",
        "IP",
        "FA_HDR_IP",
        12,
    )

    ip.AWS_Extract_Line.assert_called_once_with(
        "dc_prod_data_sources_catalog.tmsis",
        "DA_SCHEMA",
        "IP",
        "IP",
        "CIP00003",
        "TMSIS_CLL_REC_IP",
        12,
    )

    grouper.AWS_Assign_Grouper_Data_Conv.assert_called_once_with(
        "IP", "IP_HEADER", "IP_LINE", "DSCHRG_DT", True, True, True, True, True
    )

    mock_iph_class.return_value.create.assert_called_once_with(runner)
    mock_ipl_class.return_value.create.assert_called_once_with(runner)
    mock_ip_dx_class.return_value.create.assert_called_once_with(runner)

    grouper.fasc_code.assert_called_once_with("IP")

    mock_iph_class.return_value.build.assert_called_once_with(runner)
    mock_ipl_class.return_value.build.assert_called_once_with(runner)
    mock_ip_dx_class.return_value.build.assert_called_once_with(runner)
