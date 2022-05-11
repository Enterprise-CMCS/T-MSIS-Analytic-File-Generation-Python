create temp table ELG00010_multi distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    *
from
    ELG00010_multi_step2
where
    keep_flag = 1