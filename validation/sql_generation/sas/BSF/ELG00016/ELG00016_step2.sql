create temp table ELG00016_step2 distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    *
from
    ELG00016_step1
where
    keeper = 1