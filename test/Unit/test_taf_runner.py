from taf.TAF_Runner import TAF_Runner


def test_obj_name_short_sql_statement():
    sql = "drop table if exists taf_v6.numbers"

    assert TAF_Runner._obj_name(sql) is None

def test_obj_name_valid_sql_statement():
    sql = "one two three four five taf_v6.numbers"

    assert TAF_Runner._obj_name(sql) == "taf_v6.numbers"