create
or replace temporary view ELG00014_202111_uniq as
select
    t1.submtg_state_cd,
    t1.msis_ident_num,
    t1.MC_PLAN_IDENTIFIER as MC_PLAN_ID1,
    t1.ENRLD_MC_PLAN_TYPE_CODE as MC_PLAN_TYPE_CD1,
    t2.MC_PLAN_IDENTIFIER as MC_PLAN_ID2,
    t2.ENRLD_MC_PLAN_TYPE_CODE as MC_PLAN_TYPE_CD2,
    t3.MC_PLAN_IDENTIFIER as MC_PLAN_ID3,
    t3.ENRLD_MC_PLAN_TYPE_CODE as MC_PLAN_TYPE_CD3,
    t4.MC_PLAN_IDENTIFIER as MC_PLAN_ID4,
    t4.ENRLD_MC_PLAN_TYPE_CODE as MC_PLAN_TYPE_CD4,
    t5.MC_PLAN_IDENTIFIER as MC_PLAN_ID5,
    t5.ENRLD_MC_PLAN_TYPE_CODE as MC_PLAN_TYPE_CD5,
    t6.MC_PLAN_IDENTIFIER as MC_PLAN_ID6,
    t6.ENRLD_MC_PLAN_TYPE_CODE as MC_PLAN_TYPE_CD6,
    t7.MC_PLAN_IDENTIFIER as MC_PLAN_ID7,
    t7.ENRLD_MC_PLAN_TYPE_CODE as MC_PLAN_TYPE_CD7,
    t8.MC_PLAN_IDENTIFIER as MC_PLAN_ID8,
    t8.ENRLD_MC_PLAN_TYPE_CODE as MC_PLAN_TYPE_CD8,
    t9.MC_PLAN_IDENTIFIER as MC_PLAN_ID9,
    t9.ENRLD_MC_PLAN_TYPE_CODE as MC_PLAN_TYPE_CD9,
    t10.MC_PLAN_IDENTIFIER as MC_PLAN_ID10,
    t10.ENRLD_MC_PLAN_TYPE_CODE as MC_PLAN_TYPE_CD10,
    t11.MC_PLAN_IDENTIFIER as MC_PLAN_ID11,
    t11.ENRLD_MC_PLAN_TYPE_CODE as MC_PLAN_TYPE_CD11,
    t12.MC_PLAN_IDENTIFIER as MC_PLAN_ID12,
    t12.ENRLD_MC_PLAN_TYPE_CODE as MC_PLAN_TYPE_CD12,
    t13.MC_PLAN_IDENTIFIER as MC_PLAN_ID13,
    t13.ENRLD_MC_PLAN_TYPE_CODE as MC_PLAN_TYPE_CD13,
    t14.MC_PLAN_IDENTIFIER as MC_PLAN_ID14,
    t14.ENRLD_MC_PLAN_TYPE_CODE as MC_PLAN_TYPE_CD14,
    t15.MC_PLAN_IDENTIFIER as MC_PLAN_ID15,
    t15.ENRLD_MC_PLAN_TYPE_CODE as MC_PLAN_TYPE_CD15,
    t16.MC_PLAN_IDENTIFIER as MC_PLAN_ID16,
    t16.ENRLD_MC_PLAN_TYPE_CODE as MC_PLAN_TYPE_CD16
from
    (
        select
            *
        from
            ELG00014_step2
        where
            keeper = 1
    ) t1
    left join (
        select
            *
        from
            ELG00014_step2
        where
            keeper = 2
    ) t2 on t1.submtg_state_cd = t2.submtg_state_cd
    and t1.msis_ident_num = t2.msis_ident_num
    left join (
        select
            *
        from
            ELG00014_step2
        where
            keeper = 3
    ) t3 on t1.submtg_state_cd = t3.submtg_state_cd
    and t1.msis_ident_num = t3.msis_ident_num
    left join (
        select
            *
        from
            ELG00014_step2
        where
            keeper = 4
    ) t4 on t1.submtg_state_cd = t4.submtg_state_cd
    and t1.msis_ident_num = t4.msis_ident_num
    left join (
        select
            *
        from
            ELG00014_step2
        where
            keeper = 5
    ) t5 on t1.submtg_state_cd = t5.submtg_state_cd
    and t1.msis_ident_num = t5.msis_ident_num
    left join (
        select
            *
        from
            ELG00014_step2
        where
            keeper = 6
    ) t6 on t1.submtg_state_cd = t6.submtg_state_cd
    and t1.msis_ident_num = t6.msis_ident_num
    left join (
        select
            *
        from
            ELG00014_step2
        where
            keeper = 7
    ) t7 on t1.submtg_state_cd = t7.submtg_state_cd
    and t1.msis_ident_num = t7.msis_ident_num
    left join (
        select
            *
        from
            ELG00014_step2
        where
            keeper = 8
    ) t8 on t1.submtg_state_cd = t8.submtg_state_cd
    and t1.msis_ident_num = t8.msis_ident_num
    left join (
        select
            *
        from
            ELG00014_step2
        where
            keeper = 9
    ) t9 on t1.submtg_state_cd = t9.submtg_state_cd
    and t1.msis_ident_num = t9.msis_ident_num
    left join (
        select
            *
        from
            ELG00014_step2
        where
            keeper = 10
    ) t10 on t1.submtg_state_cd = t10.submtg_state_cd
    and t1.msis_ident_num = t10.msis_ident_num
    left join (
        select
            *
        from
            ELG00014_step2
        where
            keeper = 11
    ) t11 on t1.submtg_state_cd = t11.submtg_state_cd
    and t1.msis_ident_num = t11.msis_ident_num
    left join (
        select
            *
        from
            ELG00014_step2
        where
            keeper = 12
    ) t12 on t1.submtg_state_cd = t12.submtg_state_cd
    and t1.msis_ident_num = t12.msis_ident_num
    left join (
        select
            *
        from
            ELG00014_step2
        where
            keeper = 13
    ) t13 on t1.submtg_state_cd = t13.submtg_state_cd
    and t1.msis_ident_num = t13.msis_ident_num
    left join (
        select
            *
        from
            ELG00014_step2
        where
            keeper = 14
    ) t14 on t1.submtg_state_cd = t14.submtg_state_cd
    and t1.msis_ident_num = t14.msis_ident_num
    left join (
        select
            *
        from
            ELG00014_step2
        where
            keeper = 15
    ) t15 on t1.submtg_state_cd = t15.submtg_state_cd
    and t1.msis_ident_num = t15.msis_ident_num
    left join (
        select
            *
        from
            ELG00014_step2
        where
            keeper = 16
    ) t16 on t1.submtg_state_cd = t16.submtg_state_cd
    and t1.msis_ident_num = t16.msis_ident_num