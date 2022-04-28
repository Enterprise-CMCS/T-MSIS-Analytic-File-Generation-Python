create
or replace temporary view ELG00012_step1 as
select
    distinct submtg_state_cd,
    msis_ident_num,
    rec_num,
    WVR_ENRLMT_EFCTV_DT,
    WVR_ENRLMT_END_DT,
    tmsis_rptg_prd,
    wvr_id,
    (
        cast(
            case
                when length(trim(WVR_TYPE_CD)) = 1
                and WVR_TYPE_CD <> '' then lpad(WVR_TYPE_CD, 2, '0')
                else WVR_TYPE_CD
            end as char(2)
        )
    ) as WVR_TYPE_CODE,
    row_number() over (
        partition by submtg_state_cd,
        msis_ident_num,
        wvr_id,
        cast(
            case
                when length(trim(WVR_TYPE_CD)) = 1
                and WVR_TYPE_CD <> '' then lpad(WVR_TYPE_CD, 2, '0')
                else WVR_TYPE_CD
            end as char(2)
        )
        order by
            submtg_state_cd,
            msis_ident_num,
            TMSIS_RPTG_PRD desc,
            WVR_ENRLMT_EFCTV_DT desc,
            WVR_ENRLMT_END_DT desc,
            REC_NUM desc,
            wvr_id,
            cast(
                case
                    when length(trim(WVR_TYPE_CD)) = 1
                    and WVR_TYPE_CD <> '' then lpad(WVR_TYPE_CD, 2, '0')
                    else WVR_TYPE_CD
                end as char(2)
            )
    ) as waiver_deduper
from
    (
        select
            *
        from
            ELG00012
        where
            wvr_id is not null
            or wvr_type_cd is not null
    ) t1