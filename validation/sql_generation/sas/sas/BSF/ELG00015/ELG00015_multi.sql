create temp table ELG00015_multi distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    *
from
    ELG00015_multi_step2
where
    keep_flag = 1