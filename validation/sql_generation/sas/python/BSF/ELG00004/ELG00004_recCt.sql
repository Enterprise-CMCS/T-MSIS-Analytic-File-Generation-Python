create
or replace temporary view ELG00004_recCt as
select
    submtg_state_cd,
    msis_ident_num,
    count(msis_ident_num) as recCt
from
    ELG00004
where
    ELGBL_ADR_TYPE_CD in ('01', '1')
group by
    submtg_state_cd,
    msis_ident_num