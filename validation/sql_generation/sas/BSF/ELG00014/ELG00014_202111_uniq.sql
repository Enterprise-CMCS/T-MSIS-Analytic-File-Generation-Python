create temp table ELG00014_202111_uniq distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
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
    cast(null as varchar(12)) as MC_PLAN_ID8,
    cast(null as varchar(2)) as MC_PLAN_TYPE_CD8,
    cast(null as varchar(12)) as MC_PLAN_ID9,
    cast(null as varchar(2)) as MC_PLAN_TYPE_CD9,
    cast(null as varchar(12)) as MC_PLAN_ID10,
    cast(null as varchar(2)) as MC_PLAN_TYPE_CD10,
    cast(null as varchar(12)) as MC_PLAN_ID11,
    cast(null as varchar(2)) as MC_PLAN_TYPE_CD11,
    cast(null as varchar(12)) as MC_PLAN_ID12,
    cast(null as varchar(2)) as MC_PLAN_TYPE_CD12,
    cast(null as varchar(12)) as MC_PLAN_ID13,
    cast(null as varchar(2)) as MC_PLAN_TYPE_CD13,
    cast(null as varchar(12)) as MC_PLAN_ID14,
    cast(null as varchar(2)) as MC_PLAN_TYPE_CD14,
    cast(null as varchar(12)) as MC_PLAN_ID15,
    cast(null as varchar(2)) as MC_PLAN_TYPE_CD15,
    cast(null as varchar(12)) as MC_PLAN_ID16,
    cast(null as varchar(2)) as MC_PLAN_TYPE_CD16
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