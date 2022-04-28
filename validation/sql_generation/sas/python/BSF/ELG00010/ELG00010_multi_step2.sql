create
or replace temporary view ELG00010_multi_step2 as
select
    *,
    row_number() over (
        partition by submtg_state_cd,
        msis_ident_num
        order by
            submtg_state_cd,
            msis_ident_num,
            TMSIS_RPTG_PRD desc,
            MFP_ENRLMT_EFCTV_DT desc,
            MFP_ENRLMT_END_DT desc,
            REC_NUM desc,
            coalesce(trim(mfp_lvs_wth_fmly_cd), 'xx') || coalesce(trim(mfp_qlfyd_instn_cd), 'xx') || coalesce(trim(mfp_qlfyd_rsdnc_cd), 'xx') || coalesce(trim(mfp_prtcptn_endd_rsn_cd), 'xx') || coalesce(trim(mfp_rinstlzd_rsn_cd), 'xx')
    ) as KEEP_FLAG
from
    ELG00010_multi_all