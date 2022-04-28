create
or replace temporary view ELG00014_step2 as
select
    *,
    row_number() over (
        partition by submtg_state_cd,
        msis_ident_num
        order by
            submtg_state_cd,
            msis_ident_num,
            TMSIS_RPTG_PRD desc,
            MC_PLAN_ENRLMT_EFCTV_DT desc,
            MC_PLAN_ENRLMT_END_DT desc,
            REC_NUM desc,
            MC_PLAN_IDENTIFIER,
            ENRLD_MC_PLAN_TYPE_CODE
    ) as keeper
from
    (
        select
            *
        from
            ELG00014_step1
        where
            (
                enrld_mc_plan_type_code is not null
                or mc_plan_identifier is not null
            )
            and mc_deduper = 1
    ) t1