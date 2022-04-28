create
or replace temporary view ELG00021_MDCD_step1 as
select
        * --  Create a unique date ID to filter on later
,
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
        ELG00021_MDCD_step0