create temp table ELG00022_MSIS_XWALK_multi distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    *
from
    ELG00022_MSIS_XWALK_multi_step2
where
    keep_flag = 1