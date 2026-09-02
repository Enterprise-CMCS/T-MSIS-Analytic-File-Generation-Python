"""
Unit tests for BSF Metadata validation.

These tests verify that the new valid TAF values added for MDA-321
are included in the BSF Metadata validator and are recognized by
the maskInvalidValues() function.

The tests are implemented using PyTest and do not require a local
SparkSession or Java runtime.
"""

from taf.BSF.BSF_Metadata import BSF_Metadata


def test_new_valid_elgblty_trmntn_rsn_values():
    valid_values = BSF_Metadata.validator.get("ELGBLTY_TRMNTN_RSN")

    for value in ["32", "33", "34", "35", "36"]:
        assert value in valid_values


def test_new_valid_wvr_type_cd_value():
    valid_values = BSF_Metadata.validator.get("WVR_TYPE_CD")

    assert "34" in valid_values


def test_new_valid_imgrtn_stus_cd_value():
    valid_values = BSF_Metadata.validator.get("IMGRTN_STUS_CD")

    assert "4" in valid_values


def test_new_valid_elgblty_grp_cd_value():
    valid_values = BSF_Metadata.validator.get("ELGBLTY_GRP_CD")

    assert "77" in valid_values


def test_mask_invalid_values_for_new_valid_codes():
    test_cases = {
        "ELGBLTY_TRMNTN_RSN": ["32", "33", "34", "35", "36"],
        "WVR_TYPE_CD": ["34"],
        "IMGRTN_STUS_CD": ["4"],
        "ELGBLTY_GRP_CD": ["77"],
    }

    for column, new_values in test_cases.items():
        sql = BSF_Metadata.maskInvalidValues(column, "t1")

        assert f"t1.{column}" in sql
        assert "then upper(trim(t1." in sql
        assert "else null" in sql.lower()

        for value in new_values:
            assert f"'{value}'" in sql