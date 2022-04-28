create
or replace temporary view MDCD_spells as
select
      m.*,
      1 as MDCD_ENR,
      t1.ENRLMT_EFCTV_DT as MDCD_ENRLMT_EFF_DT_1,
      t1.ENRLMT_END_DT as MDCD_ENRLMT_END_DT_1,
      t2.ENRLMT_EFCTV_DT as MDCD_ENRLMT_EFF_DT_2,
      t2.ENRLMT_END_DT as MDCD_ENRLMT_END_DT_2,
      t3.ENRLMT_EFCTV_DT as MDCD_ENRLMT_EFF_DT_3,
      t3.ENRLMT_END_DT as MDCD_ENRLMT_END_DT_3,
      t4.ENRLMT_EFCTV_DT as MDCD_ENRLMT_EFF_DT_4,
      t4.ENRLMT_END_DT as MDCD_ENRLMT_END_DT_4,
      t5.ENRLMT_EFCTV_DT as MDCD_ENRLMT_EFF_DT_5,
      t5.ENRLMT_END_DT as MDCD_ENRLMT_END_DT_5,
      t6.ENRLMT_EFCTV_DT as MDCD_ENRLMT_EFF_DT_6,
      t6.ENRLMT_END_DT as MDCD_ENRLMT_END_DT_6,
      t7.ENRLMT_EFCTV_DT as MDCD_ENRLMT_EFF_DT_7,
      t7.ENRLMT_END_DT as MDCD_ENRLMT_END_DT_7,
      t8.ENRLMT_EFCTV_DT as MDCD_ENRLMT_EFF_DT_8,
      t8.ENRLMT_END_DT as MDCD_ENRLMT_END_DT_8,
      t9.ENRLMT_EFCTV_DT as MDCD_ENRLMT_EFF_DT_9,
      t9.ENRLMT_END_DT as MDCD_ENRLMT_END_DT_9,
      t10.ENRLMT_EFCTV_DT as MDCD_ENRLMT_EFF_DT_10,
      t10.ENRLMT_END_DT as MDCD_ENRLMT_END_DT_10,
      t11.ENRLMT_EFCTV_DT as MDCD_ENRLMT_EFF_DT_11,
      t11.ENRLMT_END_DT as MDCD_ENRLMT_END_DT_11,
      t12.ENRLMT_EFCTV_DT as MDCD_ENRLMT_EFF_DT_12,
      t12.ENRLMT_END_DT as MDCD_ENRLMT_END_DT_12,
      t13.ENRLMT_EFCTV_DT as MDCD_ENRLMT_EFF_DT_13,
      t13.ENRLMT_END_DT as MDCD_ENRLMT_END_DT_13,
      t14.ENRLMT_EFCTV_DT as MDCD_ENRLMT_EFF_DT_14,
      t14.ENRLMT_END_DT as MDCD_ENRLMT_END_DT_14,
      t15.ENRLMT_EFCTV_DT as MDCD_ENRLMT_EFF_DT_15,
      t15.ENRLMT_END_DT as MDCD_ENRLMT_END_DT_15,
      t16.ENRLMT_EFCTV_DT as MDCD_ENRLMT_EFF_DT_16,
      t16.ENRLMT_END_DT as MDCD_ENRLMT_END_DT_16
from
      (
            select
                  submtg_state_cd,
                  msis_ident_num,
                  sum(NUM_DAYS) as DAYS_ELIG_IN_MO_CNT,
                  max(ELIG_LAST_DAY) as ELIG_LAST_DAY
            from
                  ELG00021_MDCD_step4
            group by
                  submtg_state_cd,
                  msis_ident_num
      ) m
      left join (
            select
                  *
            from
                  ELG00021_MDCD_step4
            where
                  keeper = 1
      ) t1 on t1.submtg_state_cd = t1.submtg_state_cd
      and t1.msis_ident_num = t1.msis_ident_num
      left join (
            select
                  *
            from
                  ELG00021_MDCD_step4
            where
                  keeper = 2
      ) t2 on t1.submtg_state_cd = t2.submtg_state_cd
      and t1.msis_ident_num = t2.msis_ident_num
      left join (
            select
                  *
            from
                  ELG00021_MDCD_step4
            where
                  keeper = 3
      ) t3 on t1.submtg_state_cd = t3.submtg_state_cd
      and t1.msis_ident_num = t3.msis_ident_num
      left join (
            select
                  *
            from
                  ELG00021_MDCD_step4
            where
                  keeper = 4
      ) t4 on t1.submtg_state_cd = t4.submtg_state_cd
      and t1.msis_ident_num = t4.msis_ident_num
      left join (
            select
                  *
            from
                  ELG00021_MDCD_step4
            where
                  keeper = 5
      ) t5 on t1.submtg_state_cd = t5.submtg_state_cd
      and t1.msis_ident_num = t5.msis_ident_num
      left join (
            select
                  *
            from
                  ELG00021_MDCD_step4
            where
                  keeper = 6
      ) t6 on t1.submtg_state_cd = t6.submtg_state_cd
      and t1.msis_ident_num = t6.msis_ident_num
      left join (
            select
                  *
            from
                  ELG00021_MDCD_step4
            where
                  keeper = 7
      ) t7 on t1.submtg_state_cd = t7.submtg_state_cd
      and t1.msis_ident_num = t7.msis_ident_num
      left join (
            select
                  *
            from
                  ELG00021_MDCD_step4
            where
                  keeper = 8
      ) t8 on t1.submtg_state_cd = t8.submtg_state_cd
      and t1.msis_ident_num = t8.msis_ident_num
      left join (
            select
                  *
            from
                  ELG00021_MDCD_step4
            where
                  keeper = 9
      ) t9 on t1.submtg_state_cd = t9.submtg_state_cd
      and t1.msis_ident_num = t9.msis_ident_num
      left join (
            select
                  *
            from
                  ELG00021_MDCD_step4
            where
                  keeper = 10
      ) t10 on t1.submtg_state_cd = t10.submtg_state_cd
      and t1.msis_ident_num = t10.msis_ident_num
      left join (
            select
                  *
            from
                  ELG00021_MDCD_step4
            where
                  keeper = 11
      ) t11 on t1.submtg_state_cd = t11.submtg_state_cd
      and t1.msis_ident_num = t11.msis_ident_num
      left join (
            select
                  *
            from
                  ELG00021_MDCD_step4
            where
                  keeper = 12
      ) t12 on t1.submtg_state_cd = t12.submtg_state_cd
      and t1.msis_ident_num = t12.msis_ident_num
      left join (
            select
                  *
            from
                  ELG00021_MDCD_step4
            where
                  keeper = 13
      ) t13 on t1.submtg_state_cd = t13.submtg_state_cd
      and t1.msis_ident_num = t13.msis_ident_num
      left join (
            select
                  *
            from
                  ELG00021_MDCD_step4
            where
                  keeper = 14
      ) t14 on t1.submtg_state_cd = t14.submtg_state_cd
      and t1.msis_ident_num = t14.msis_ident_num
      left join (
            select
                  *
            from
                  ELG00021_MDCD_step4
            where
                  keeper = 15
      ) t15 on t1.submtg_state_cd = t15.submtg_state_cd
      and t1.msis_ident_num = t15.msis_ident_num
      left join (
            select
                  *
            from
                  ELG00021_MDCD_step4
            where
                  keeper = 16
      ) t16 on t1.submtg_state_cd = t16.submtg_state_cd
      and t1.msis_ident_num = t16.msis_ident_num