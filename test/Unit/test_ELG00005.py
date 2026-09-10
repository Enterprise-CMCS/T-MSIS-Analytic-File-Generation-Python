from unittest.mock import MagicMock

from taf.BSF.ELG00005 import ELG00005


def _get_created_vars():
    """
    Generate the ELG00005 SQL without running the TAF/BSF processing framework.
    """
    # Create the object without running ELG.__init__(), which would
    # initialize the TAF/BSF processing framework.
    elg = object.__new__(ELG00005)

    # Mock BSF operations
    
    elg.tab_no = "TEST"
    elg.bsf = MagicMock()
    elg.MultiIds = MagicMock()

    # Generate the SQL containing the eligibility group category mapping.
    elg.create()

    # The first argument passed to MultiIds() is the created_vars SQL.
    return elg.MultiIds.call_args.args[0]


def test_eligibility_group_category_mapping():
    """
    Verify all ELGBLTY_GRP_CD to eligibility group category mappings.
    """
    created_vars = _get_created_vars()

    # Category 1
    assert "between '01' and '09'" in created_vars
    assert "between '72' and '75'" in created_vars

    # Category 2
    assert "between '11' and '19'" in created_vars
    assert "between '20' and '26'" in created_vars

    # Category 3
    assert "between '27' and '29'" in created_vars
    assert "between '30' and '36'" in created_vars

    # MDA-1227: eligibility group codes 76 and 77 map to category 3.
    assert "in ('76','77')" in created_vars

    # Category 4
    assert "between '37' and '39'" in created_vars
    assert "between '40' and '49'" in created_vars
    assert "between '50' and '52'" in created_vars

    # Category 5
    assert "between '53' and '56'" in created_vars

    # Category 6
    assert "in('59','60')" in created_vars

    # Category 7
    assert "in('61','62','63')" in created_vars

    # Category 8
    assert "in('64','65','66')" in created_vars

    # Category 9
    assert "in('67','68')" in created_vars

    # Category 10
    assert "in('69','70','71')" in created_vars


def test_invalid_eligibility_group_code_78_is_not_mapped_to_category_3():
    """
    Verify that eligibility group code 78 is not included in the
    category 3 mapping.
    """
    created_vars = _get_created_vars()

    # Extract the category 3 portion of the CASE statement.
    category_3_start = created_vars.index("then 2") + len("then 2")
    category_3_end = created_vars.index("then 3", category_3_start)

    category_3_mapping = created_vars[category_3_start:category_3_end]

    # 78 is not a category 3 eligibility group code.
    assert "'78'" not in category_3_mapping
