create
or replace temporary view ELG00010_recCt as
select
    submtg_state_cd,
    msis_ident_num,
    count(TMSIS_RUN_ID) as recCt
from
    ELG00010
group by
    submtg_state_cd,
    msis_ident_num