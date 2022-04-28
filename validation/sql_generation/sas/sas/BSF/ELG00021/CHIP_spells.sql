create temp table CHIP_spells distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    m.*,
    1 as CHIP_ENR,
    t1.ENRLMT_EFCTV_DT as CHIP_ENRLMT_EFF_DT_1,
    t1.ENRLMT_END_DT as CHIP_ENRLMT_END_DT_1,
    t2.ENRLMT_EFCTV_DT as CHIP_ENRLMT_EFF_DT_2,
    t2.ENRLMT_END_DT as CHIP_ENRLMT_END_DT_2,
    cast(null as date) as CHIP_ENRLMT_EFF_DT_3,
    cast(null as date) as CHIP_ENRLMT_END_DT_3,
    cast(null as date) as CHIP_ENRLMT_EFF_DT_4,
    cast(null as date) as CHIP_ENRLMT_END_DT_4,
    cast(null as date) as CHIP_ENRLMT_EFF_DT_5,
    cast(null as date) as CHIP_ENRLMT_END_DT_5,
    cast(null as date) as CHIP_ENRLMT_EFF_DT_6,
    cast(null as date) as CHIP_ENRLMT_END_DT_6,
    cast(null as date) as CHIP_ENRLMT_EFF_DT_7,
    cast(null as date) as CHIP_ENRLMT_END_DT_7,
    cast(null as date) as CHIP_ENRLMT_EFF_DT_8,
    cast(null as date) as CHIP_ENRLMT_END_DT_8,
    cast(null as date) as CHIP_ENRLMT_EFF_DT_9,
    cast(null as date) as CHIP_ENRLMT_END_DT_9,
    cast(null as date) as CHIP_ENRLMT_EFF_DT_10,
    cast(null as date) as CHIP_ENRLMT_END_DT_10,
    cast(null as date) as CHIP_ENRLMT_EFF_DT_11,
    cast(null as date) as CHIP_ENRLMT_END_DT_11,
    cast(null as date) as CHIP_ENRLMT_EFF_DT_12,
    cast(null as date) as CHIP_ENRLMT_END_DT_12,
    cast(null as date) as CHIP_ENRLMT_EFF_DT_13,
    cast(null as date) as CHIP_ENRLMT_END_DT_13,
    cast(null as date) as CHIP_ENRLMT_EFF_DT_14,
    cast(null as date) as CHIP_ENRLMT_END_DT_14,
    cast(null as date) as CHIP_ENRLMT_EFF_DT_15,
    cast(null as date) as CHIP_ENRLMT_END_DT_15,
    cast(null as date) as CHIP_ENRLMT_EFF_DT_16,
    cast(null as date) as CHIP_ENRLMT_END_DT_16
from
    (
        select
            submtg_state_cd,
            msis_ident_num,
            sum(NUM_DAYS) as DAYS_ELIG_IN_MO_CNT,
            max(ELIG_LAST_DAY) as ELIG_LAST_DAY
        from
            ELG00021_CHIP_step4
        group by
            submtg_state_cd,
            msis_ident_num
    ) m
    left join (
        select
            *
        from
            ELG00021_CHIP_step4
        where
            keeper = 1
    ) t1 on m.submtg_state_cd = t1.submtg_state_cd
    and m.msis_ident_num = t1.msis_ident_num
    left join (
        select
            *
        from
            ELG00021_CHIP_step4
        where
            keeper = 2
    ) t2 on m.submtg_state_cd = t2.submtg_state_cd
    and m.msis_ident_num = t2.msis_ident_num