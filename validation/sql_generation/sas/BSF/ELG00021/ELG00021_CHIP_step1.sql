create temp table ELG00021_CHIP_step1 distkey(dateId) sortkey(submtg_state_cd, msis_ident_num) as
select
    *,
    trim(
        submtg_state_cd || '-' || msis_ident_num || '-' || cast(
            rank() over (
                partition by submtg_state_cd,
                msis_ident_num
                order by
                    submtg_state_cd,
                    msis_ident_num,
                    ENRLMT_EFCTV_DT,
                    ENRLMT_END_DT
            ) as char(3)
        )
    ) as dateId
from
    ELG00021_CHIP_step0