create temp table ELG00022_ADDTNL_multi_step2 distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num, keep_flag) as
select
    *,
    row_number() over (
        partition by submtg_state_cd,
        msis_ident_num
        order by
            submtg_state_cd,
            msis_ident_num,
            TMSIS_RPTG_PRD desc,
            ELGBL_ID_EFCTV_DT desc,
            ELGBL_ID_END_DT desc,
            REC_NUM desc,
            coalesce(trim(elgbl_id), 'xx') || coalesce(trim(elgbl_id_issg_ent_id_txt), 'xx') || coalesce(trim(rsn_for_chg), 'xx')
    ) as KEEP_FLAG
from
    ELG00022_ADDTNL_multi_all
where
    ELGBL_ID_TYPE_CD = '1'
    and nullif(trim(elgbl_id), '') is not null