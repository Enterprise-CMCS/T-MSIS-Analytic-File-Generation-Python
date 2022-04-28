create temp table ELG00022_MSIS_XWALK_uniq distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    t1.*,
    ELGBL_ID as ELGBL_ID_MSIS_XWALK,
    ELGBL_ID_ISSG_ENT_ID_TXT as ELGBL_ID_MSIS_XWALK_ENT_ID,
    RSN_FOR_CHG as ELGBL_ID_MSIS_XWALK_RSN_CHG,
    1 as KEEP_FLAG
from
    ELG00022 t1
    inner join ELG00022_MSIS_XWALK_recCt t2 on t1.submtg_state_cd = t2.submtg_state_cd
    and t1.msis_ident_num = t2.msis_ident_num
    and t2.recCt = 1
where
    ELGBL_ID_TYPE_CD = '2'
    and nullif(trim(elgbl_id), '') is not null