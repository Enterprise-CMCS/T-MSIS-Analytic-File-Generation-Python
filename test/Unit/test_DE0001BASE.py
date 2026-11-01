"""Unit tests for taf.DE.DE0001BASE."""

from unittest.mock import MagicMock, patch

from taf.DE.DE0001BASE import DE0001BASE


# Expected CE source fields added to the Base data element.
CE_FIELDS = (
    "ADLT_GRP_QUALN_CD",
    "APLCBL_INDVDL_QUALN_TYPE_CD",
    "APLCBL_INDVDL_EXCPTN_CD",
    "EXCLSN_TYPE_CD",
    "MDCL_FRLTY_TYPE_CD",
    "WORK_FLAG",
    "CMNTY_SRVC_FLAG",
    "JOBS_PGM_FLAG",
    "EDCTN_FLAG",
    "INCM_FLAG",
)


def create_base_instance():
    """Create a DE0001BASE instance with a mocked runner."""
    runner = MagicMock()
    return DE0001BASE(runner)


def expected_ce_columns():
    """Build the expected monthly and latest CE output column names."""
    columns = []

    # Each CE field has 12 monthly columns, from _01 through _12.
    for field in CE_FIELDS:
        for month in range(1, 13):
            columns.append(f"{field}_{month:02d}")

        # Each CE field also has one latest non-null output column.
        columns.append(f"{field}_LTST")

    return columns


def test_ce_fields_contains_all_expected_fields():
    """Verify that the class defines the 10 expected CE source fields."""
    assert DE0001BASE.CE_FIELDS == CE_FIELDS
    assert len(DE0001BASE.CE_FIELDS) == 10


def test_expected_ce_columns_contains_130_unique_columns():
    """Verify that the CE fields produce 130 unique output columns."""
    columns = expected_ce_columns()

    # 10 CE fields multiplied by 13 outputs per field equals 130.
    assert len(columns) == 130
    assert len(set(columns)) == 130


def test_basecols_includes_all_ce_columns():
    """Verify that basecols() includes every monthly and latest CE column."""
    base = create_base_instance()

    # Generate the Base output column list.
    columns_sql = base.basecols()

    # Every expected CE output column must appear in the generated list.
    for column in expected_ce_columns():
        assert f",{column}" in columns_sql, (
            f"Expected CE column {column} was not found in basecols()."
        )


@patch("taf.DE.DE0001BASE.TAF_Closure.monthly_array")
@patch("taf.DE.DE0001BASE.DE.last_best")
def test_create_temp_generates_monthly_and_latest_ce_expressions(
    mock_last_best, mock_monthly_array
):
    """Verify that create_temp() generates expressions for every CE field."""

    # Return recognizable SQL snippets so the generated SQL can be inspected.
    # Accept optional arguments used by existing Base fields as well as CE fields.
    mock_monthly_array.side_effect = (
        lambda instance, incol, outcol=None: f"{outcol or incol}_MONTHLY_SQL"
    )
    mock_last_best.side_effect = (
        lambda instance, incol, outcol=None: f"{outcol}_LATEST_SQL"
    )

    base = create_base_instance()

    # create_temp() builds SQL and sends it to create_temp_table().
    # Mock table creation so no database or Spark execution is attempted.
    base.create_temp_table = MagicMock()

    # Mock unrelated SQL helper methods used while constructing the query.
    with patch("taf.DE.DE0001BASE.DE.mc_type_rank", return_value=""):
        base.create_temp()

        # Collect the arguments passed to the monthly and latest-value helpers.
    # Existing Base fields may use positional or keyword arguments.
    monthly_fields = [
        call.kwargs.get("incol", call.args[1] if len(call.args) > 1 else None)
        for call in mock_monthly_array.call_args_list
    ]

    latest_fields = [
        call.kwargs.get("incol", call.args[1] if len(call.args) > 1 else None)
        for call in mock_last_best.call_args_list
    ]

    latest_output_fields = [
        call.kwargs.get("outcol")
        for call in mock_last_best.call_args_list
    ]

    # Verify every CE source field is processed by both helpers.
    for field in CE_FIELDS:
        assert field in monthly_fields
        assert field in latest_fields
        assert f"{field}_LTST" in latest_output_fields

        # Verify that temporary-table creation was invoked.
    base.create_temp_table.assert_called_once()

    # Confirm that every CE field was passed to both SQL-generation helpers.
    for field in CE_FIELDS:
        assert field in monthly_fields
        assert field in latest_fields
        assert f"{field}_LTST" in latest_output_fields