create
or replace temporary view ELG00022_ADDTNL_multi_all as
select
    t1.*,
    ELGBL_ID as ELGBL_ID_ & id_type,
    ELGBL_ID_ISSG_ENT_ID_TXT as ELGBL_ID_ADDTNL_ENT_ID,
    RSN_FOR_CHG as ELGBL_ID_ADDTNL_RSN_CHG
from
    ELG00022 as t1
    inner join ELG00022_ADDTNL_recCt as t2 on t1.submtg_state_cd = t2.submtg_state_cd
    and t1.msis_ident_num = t2.msis_ident_num
    and t2.recCt > 1
where
    ELGBL_ID_TYPE_CD = 1
    and nullif(trim(elgbl_id), '') is not null
)