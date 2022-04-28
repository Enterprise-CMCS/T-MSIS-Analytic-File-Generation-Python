create temp table ELG00003_multi_step2 distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num, keep_flag) as
select
    *,
    row_number() over (
        partition by submtg_state_cd,
        msis_ident_num
        order by
            submtg_state_cd,
            msis_ident_num,
            TMSIS_RPTG_PRD desc,
            VAR_DMGRPHC_ELE_EFCTV_DT desc,
            VAR_DMGRPHC_ELE_END_DT desc,
            REC_NUM desc,
            coalesce(mrtl_stus_cd, 'xx') || coalesce(cast(ssn_num as char(10)), 'xx') || coalesce(incm_cd, 'xx') || coalesce(vet_ind, 'xx') || coalesce(ctznshp_ind, 'xx') || coalesce(imgrtn_stus_cd, 'xx') || coalesce(upper(prmry_lang_cd), 'xx') || coalesce(hsehld_size_cd, 'xx') || coalesce(mdcr_hicn_num, 'xx') || coalesce(chip_cd, 'xx') || coalesce(prmry_lang_englsh_prfcncy_cd, 'xx')
    ) as KEEP_FLAG
from
    ELG00003_multi_all