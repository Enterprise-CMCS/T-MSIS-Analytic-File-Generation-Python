create
or replace temporary view TPL00002_recCt as
select
    submtg_state_cd,
    msis_ident_num,
    count(TMSIS_RUN_ID) as recCt
from
    TPL00002
group by
    submtg_state_cd,
    msis_ident_num