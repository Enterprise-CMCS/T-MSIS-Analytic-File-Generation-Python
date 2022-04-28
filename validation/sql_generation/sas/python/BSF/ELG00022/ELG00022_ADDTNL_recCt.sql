create
or replace temporary view ELG00022_ADDTNL_recCt as
select
    submtg_state_cd,
    msis_ident_num,
    count(msis_ident_num) as recCt
from
    ELG00022
where
    ELGBL_ID_TYPE_CD = 1
    and nullif(trim(elgbl_id), '') is not null
group by
    submtg_state_cd,
    msis_ident_num