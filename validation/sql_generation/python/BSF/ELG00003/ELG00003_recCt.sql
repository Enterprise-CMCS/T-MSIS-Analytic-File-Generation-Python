create
or replace temporary view ELG00003_recCt as
select
    submtg_state_cd,
    msis_ident_num,
    count(TMSIS_RUN_ID) as recCt
from
    ELG00003_v
group by
    submtg_state_cd,
    msis_ident_num