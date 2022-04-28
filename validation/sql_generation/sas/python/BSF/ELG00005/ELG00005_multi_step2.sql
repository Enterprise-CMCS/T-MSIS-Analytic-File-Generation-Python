create
or replace temporary view ELG00005_multi_step2 as
select
    *,
    row_number() over (
        partition by submtg_state_cd,
        msis_ident_num
        order by
            submtg_state_cd,
            msis_ident_num,
            TMSIS_RPTG_PRD desc,
            ELGBLTY_DTRMNT_EFCTV_DT desc,
            ELGBLTY_DTRMNT_END_DT desc,
            REC_NUM desc,
            coalesce(trim(MSIS_CASE_NUM), 'x') || coalesce(trim(elgblty_mdcd_basis_cd), 'x') || coalesce(trim(dual_elgbl_cd), 'x') || coalesce(trim(elgblty_grp_cd), 'x') || coalesce(trim(care_lvl_stus_cd), 'x') || coalesce(trim(ssdi_ind), 'x') || coalesce(trim(ssi_ind), 'x') || coalesce(trim(ssi_state_splmt_stus_cd), 'x') || coalesce(trim(ssi_stus_cd), 'x') || coalesce(trim(state_spec_elgblty_fctr_txt), 'x') || coalesce(trim(birth_cncptn_ind), 'x') || coalesce(trim(mas_cd), 'x') || coalesce(trim(rstrctd_bnfts_cd), 'x') || coalesce(trim(tanf_cash_cd), 'x') || coalesce(trim(prmry_elgblty_grp_ind), 'x')
    ) as KEEP_FLAG
from
    ELG00005_multi_all
where
    PRMRY_ELGBLTY_GRP_IND = '1'