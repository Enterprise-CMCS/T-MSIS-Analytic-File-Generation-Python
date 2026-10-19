"""Unit test for the IP runner's orchestration logic."""

from unittest.mock import patch

from taf.IP.IP_Runner import IP_Runner


def test_init_calls_expected_build_steps():
    # PURPOSE:
    # Verify that IP_Runner.init() coordinates the expected IP processing
    # components and passes the correct arguments to them.

    # SETUP:
    # Create the runner without running its full initializer.
    # Use a sample schema so the test does not need the full TAF environment.
    runner = IP_Runner.__new__(IP_Runner)
    runner.DA_SCHEMA = "DA_SCHEMA"

    # MOCK DEPENDENCIES:
    # Replace dependent classes with mocks. This lets us test the real
    # runner logic without executing claims, grouper, or IP processing,
    # and without requiring Spark or a JVM.
    with (
        patch("taf.IP.IP_Runner.TAF_Grouper") as mock_grouper_class,
        patch("taf.IP.IP_Runner.TAF_Claims") as mock_claims_class,
        patch("taf.IP.IP.IP") as mock_ip_class,
        patch("taf.IP.IPH.IPH") as mock_iph_class,
        patch("taf.IP.IPL.IPL") as mock_ipl_class,
        patch("taf.IP.IP_DX.IP_DX") as mock_ip_dx_class,
    ):
        # EXECUTE:
        # Run the actual init() method while its dependencies are mocked.
        runner.init()

    # ACCESS MOCK OBJECTS:
    # Get the pretend component objects created by the runner so we can
    # verify which methods were called and what arguments they received.
    grouper = mock_grouper_class.return_value
    claims = mock_claims_class.return_value
    ip = mock_ip_class.return_value

    # VERIFY GROUPER SETUP:
    # Confirm that NPPES and CCS processing are requested for IP.
    grouper.fetch_nppes.assert_called_once_with("IP")
    grouper.fetch_ccs.assert_called_once_with("IP")

    # VERIFY CLAIMS PROCESSING:
    # Confirm the runner requests claim-family processing with the
    # expected source schema, segment, table, file type, and date column.
    claims.AWS_Claims_Family_Table_Link.assert_called_once_with(
        "dc_prod_data_sources_catalog.tmsis",
        "CIP00002",
        "TMSIS_CLH_REC_IP",
        "IP",
        "DSCHRG_DT",
    )

    # VERIFY DIAGNOSIS SQL SETUP:
    # Confirm the runner asks IP to prepare diagnosis SQL using the
    # expected source table, metadata segment, header input, and 12 fields.
    ip.select_dx.assert_called_once_with(
        "dc_prod_data_sources_catalog.tmsis",
        "CIP00004",
        "tmsis_clm_dx_ip",
        "IP",
        "FA_HDR_IP",
        12,
    )

    # VERIFY LINE SQL SETUP:
    # Confirm the runner asks IP to prepare line SQL using the expected
    # schemas, temporary table, metadata segment, source table, and count.
    ip.AWS_Extract_Line.assert_called_once_with(
        "dc_prod_data_sources_catalog.tmsis",
        "DA_SCHEMA",
        "IP",
        "IP",
        "CIP00003",
        "TMSIS_CLL_REC_IP",
        12,
    )

    # VERIFY GROUPER CONVERSION:
    # Confirm the runner requests the expected header/line conversion
    # settings and passes the configured Boolean flags.
    grouper.AWS_Assign_Grouper_Data_Conv.assert_called_once_with(
        "IP", "IP_HEADER", "IP_LINE", "DSCHRG_DT", True, True, True, True, True
    )

    # VERIFY COMPONENT CREATION:
    # Confirm the IP header, line, and diagnosis components each receive
    # their create() call with this runner.
    mock_iph_class.return_value.create.assert_called_once_with(runner)
    mock_ipl_class.return_value.create.assert_called_once_with(runner)
    mock_ip_dx_class.return_value.create.assert_called_once_with(runner)

    # VERIFY FASC PROCESSING:
    # Confirm the grouper's FASC-related method is called for IP.
    grouper.fasc_code.assert_called_once_with("IP")

    # VERIFY COMPONENT BUILD:
    # Confirm the header, line, and diagnosis components each receive
    # their build() call with this runner.
    mock_iph_class.return_value.build.assert_called_once_with(runner)
    mock_ipl_class.return_value.build.assert_called_once_with(runner)
    mock_ip_dx_class.return_value.build.assert_called_once_with(runner)
