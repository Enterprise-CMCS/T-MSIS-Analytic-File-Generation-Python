create temp table ELG00022_202111_uniq distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    coalesce(t1.msis_ident_num, t2.msis_ident_num) as msis_ident_num,
    coalesce(t1.submtg_state_cd, t2.submtg_state_cd) as submtg_state_cd,
    t1.ELGBL_ID_ADDTNL,
    t1.ELGBL_ID_ADDTNL_ENT_ID,
    t1.ELGBL_ID_ADDTNL_RSN_CHG,
    t2.ELGBL_ID_MSIS_XWALK,
    t2.ELGBL_ID_MSIS_XWALK_ENT_ID,
    t2.ELGBL_ID_MSIS_XWALK_RSN_CHG
from
    ELG00022_uniq_step1 t1 full
    join ELG00022_uniq_step2 t2 on t1.msis_ident_num = t2.msis_ident_num
    and t1.submtg_state_cd = t2.submtg_state_cd