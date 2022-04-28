create temp table ELG00021_step1 distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    *,
    row_number() over (
        partition by submtg_state_cd,
        msis_ident_num
        order by
            submtg_state_cd,
            msis_ident_num,
            TMSIS_RPTG_PRD desc,
            ENRLMT_EFCTV_DT desc,
            ENRLMT_END_DT desc,
            REC_NUM desc,
            coalesce(enrlmt_type_cd, '999')
    ) as ORDER_FLAG
from
    ELG00021