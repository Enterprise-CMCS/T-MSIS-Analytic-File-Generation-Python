create temp table ELG00021_CHIP_step3 distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    submtg_state_cd,
    msis_ident_num,
    min(ENRLMT_EFCTV_DT) as ENRLMT_EFCTV_DT,
    max(ENRLMT_END_DT) as ENRLMT_END_DT
from
    (
        select
            submtg_state_cd,
            msis_ident_num,
            ENRLMT_EFCTV_DT,
            ENRLMT_END_DT,
            sum(C) over (
                partition by submtg_state_cd,
                msis_ident_num
                order by
                    ENRLMT_EFCTV_DT,
                    ENRLMT_END_DT rows UNBOUNDED PRECEDING
            ) as G
        from
            (
                select
                    submtg_state_cd,
                    msis_ident_num,
                    ENRLMT_EFCTV_DT,
                    ENRLMT_END_DT,
                    m_eff_dt,
                    m_end_dt,
                    decode(
                        sign(ENRLMT_EFCTV_DT - nvl(m_end_dt + 1, ENRLMT_EFCTV_DT)),
                        1,
                        1,
                        0
                    ) as C
                from
                    (
                        select
                            submtg_state_cd,
                            msis_ident_num,
                            ENRLMT_EFCTV_DT,
                            ENRLMT_END_DT,
                            lag(ENRLMT_EFCTV_DT) over (
                                partition by submtg_state_cd,
                                msis_ident_num
                                order by
                                    ENRLMT_EFCTV_DT,
                                    ENRLMT_END_DT
                            ) as m_eff_dt,
                            lag(ENRLMT_END_DT) over (
                                partition by submtg_state_cd,
                                msis_ident_num
                                order by
                                    ENRLMT_EFCTV_DT,
                                    ENRLMT_END_DT
                            ) as m_end_dt
                        from
                            ELG00021_CHIP_step2
                        order by
                            ENRLMT_EFCTV_DT,
                            ENRLMT_END_DT
                    ) s1
            ) s2
    ) s3
group by
    submtg_state_cd,
    msis_ident_num,
    g