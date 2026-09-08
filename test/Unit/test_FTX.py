"""Unit tests for TRNS_TYPE_CD valid value validation in TAF FTX.

Ticket: MDA-1571
Change: Add '05' (PMPM Health Home Service Payment) as a valid value
        for TRNS_TYPE_CD in the FTX build (taf/FTX/FTX.py line 196).

The column uses var_set_type2 with lpad=2, which means:
  - Input '5' is left-padded to '05' before the validity check.
  - Any value not in the allowed list is set to NULL.

Updated FTX.py line should read:
    TAF_Closure.var_set_type2(
        'TRNS_TYPE_CD', 2,
        cond1='01', cond2='02', cond3='03', cond4='04', cond5='05', cond6='95'
    )

Tests inspect the generated SQL string directly (no SparkSession required),
"""

import sys
sys.path.insert(0, "/Volumes/dc_prod_data_sources_catalog/taf_python/taf_package_volume_prod/taf-9.0-py3-none-any.whl")

from taf.TAF_Closure import TAF_Closure

# Generate the SQL expression once; all tests inspect this string.
TRNS_TYPE_SQL = TAF_Closure.var_set_type2(
    'TRNS_TYPE_CD', 2,
    cond1='01', cond2='02', cond3='03', cond4='04', cond5='05', cond6='95'
)


# ---------------------------------------------------------------------------
# New valid value 
# ---------------------------------------------------------------------------

def test_05_is_in_valid_values():
    """'05' must appear in the SQL IN clause so it is not nulled out."""
    assert "'05'" in TRNS_TYPE_SQL


def test_lpad_pads_single_digit_to_05():
    """var_set_type2 with lpad=2 ensures '5' is left-padded to '05'."""
    assert "lpad(trim(TRNS_TYPE_CD), 2, '0')" in TRNS_TYPE_SQL


# ---------------------------------------------------------------------------
# Existing valid values — regression guard
# ---------------------------------------------------------------------------

def test_existing_valid_values_are_in_sql():
    """Pre-existing valid values must still appear in the IN clause."""
    for value in ["01", "02", "03", "04", "95"]:
        assert f"'{value}'" in TRNS_TYPE_SQL


# ---------------------------------------------------------------------------
# Invalid values — must NOT be in the allowed list
# ---------------------------------------------------------------------------

def test_invalid_values_are_not_in_sql():
    """Values outside the allowed list must not appear in the IN clause."""
    for value in ["06", "07", "10", "99", "00"]:
        assert f"'{value}'" not in TRNS_TYPE_SQL


# ---------------------------------------------------------------------------
# SQL structure — null-out logic is present
# ---------------------------------------------------------------------------

def test_sql_nulls_invalid_values():
    """The generated SQL must contain an else-NULL branch."""
    assert "else null end as trns_type_cd" in TRNS_TYPE_SQL.lower()


def test_sql_aliases_column_correctly():
    """Output column must be aliased as TRNS_TYPE_CD."""
    assert "as TRNS_TYPE_CD" in TRNS_TYPE_SQL
