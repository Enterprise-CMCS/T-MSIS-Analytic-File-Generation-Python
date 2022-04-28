create
or replace temporary view TPL00002_multi_step2 as
select
    *,
    row_number() over (
        partition by submtg_state_cd,
        msis_ident_num
        order by
            submtg_state_cd,
            msis_ident_num,
            TMSIS_RPTG_PRD desc,
            ELGBL_PRSN_MN_EFCTV_DT desc,
            ELGBL_PRSN_MN_END_DT desc,
            REC_NUM desc,
            coalesce(trim(tpl_insrnc_cvrg_ind), 'xx') || coalesce(trim(tpl_othr_cvrg_ind), 'xx')
    ) as KEEP_FLAG
from
    TPL00002_multi_all