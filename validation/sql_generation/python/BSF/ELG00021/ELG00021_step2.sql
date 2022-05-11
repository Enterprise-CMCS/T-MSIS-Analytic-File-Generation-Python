create
or replace temporary view ELG00021_step2 as
select
    t1.*,
    case
        when (
            (
                t1.ENRLMT_EFCTV_DT between t2.ENRLMT_EFCTV_DT
                and t2.ENRLMT_END_DT
            )
            or (
                t2.ENRLMT_EFCTV_DT between t1.ENRLMT_EFCTV_DT
                and t1.ENRLMT_END_DT
            )
        )
        and t1.ORDER_FLAG > t2.ORDER_FLAG then 1
        else 0
    end as OVR_LAP_RMV
from
    ELG00021_step1 t1
    left join ELG00021_step1 t2 on t1.submtg_state_cd = t2.submtg_state_cd
    and t1.msis_ident_num = t2.msis_ident_num
    and coalesce(t1.enrlmt_type_cd, 'X') <> coalesce(t2.enrlmt_type_cd, 'X')
where
    -- OVR_LAP_RMV = 1 -- Only keep records to be removed in this table
    (
        (
            (
                t1.ENRLMT_EFCTV_DT between t2.ENRLMT_EFCTV_DT
                and t2.ENRLMT_END_DT
            )
            or (
                t2.ENRLMT_EFCTV_DT between t1.ENRLMT_EFCTV_DT
                and t1.ENRLMT_END_DT
            )
        )
        and t1.ORDER_FLAG > t2.ORDER_FLAG
    )