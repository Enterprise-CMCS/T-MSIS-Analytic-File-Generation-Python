create temp table ELG00002_multi distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    *
from
    ELG00002_multi_step2
where
    keep_flag = 1