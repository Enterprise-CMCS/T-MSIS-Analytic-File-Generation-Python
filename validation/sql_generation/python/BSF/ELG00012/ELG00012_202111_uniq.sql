create
or replace temporary view ELG00012_202111_uniq as
select
    t1.submtg_state_cd,
    t1.msis_ident_num,
    t1.WVR_ID as WVR_ID1,
    t1.WVR_TYPE_CODE as WVR_TYPE_CD1,
    t2.WVR_ID as WVR_ID2,
    t2.WVR_TYPE_CODE as WVR_TYPE_CD2,
    t3.WVR_ID as WVR_ID3,
    t3.WVR_TYPE_CODE as WVR_TYPE_CD3,
    t4.WVR_ID as WVR_ID4,
    t4.WVR_TYPE_CODE as WVR_TYPE_CD4,
    t5.WVR_ID as WVR_ID5,
    t5.WVR_TYPE_CODE as WVR_TYPE_CD5,
    t6.WVR_ID as WVR_ID6,
    t6.WVR_TYPE_CODE as WVR_TYPE_CD6,
    t7.WVR_ID as WVR_ID7,
    t7.WVR_TYPE_CODE as WVR_TYPE_CD7,
    t8.WVR_ID as WVR_ID8,
    t8.WVR_TYPE_CODE as WVR_TYPE_CD8,
    t9.WVR_ID as WVR_ID9,
    t9.WVR_TYPE_CODE as WVR_TYPE_CD9,
    t10.WVR_ID as WVR_ID10,
    t10.WVR_TYPE_CODE as WVR_TYPE_CD10
from
    (
        select
            *
        from
            ELG00012_step2
        where
            keeper = 1
    ) t1
    left join (
        select
            *
        from
            ELG00012_step2
        where
            keeper = 2
    ) t2 on t1.submtg_state_cd = t2.submtg_state_cd
    and t1.msis_ident_num = t2.msis_ident_num
    left join (
        select
            *
        from
            ELG00012_step2
        where
            keeper = 3
    ) t3 on t1.submtg_state_cd = t3.submtg_state_cd
    and t1.msis_ident_num = t3.msis_ident_num
    left join (
        select
            *
        from
            ELG00012_step2
        where
            keeper = 4
    ) t4 on t1.submtg_state_cd = t4.submtg_state_cd
    and t1.msis_ident_num = t4.msis_ident_num
    left join (
        select
            *
        from
            ELG00012_step2
        where
            keeper = 5
    ) t5 on t1.submtg_state_cd = t5.submtg_state_cd
    and t1.msis_ident_num = t5.msis_ident_num
    left join (
        select
            *
        from
            ELG00012_step2
        where
            keeper = 6
    ) t6 on t1.submtg_state_cd = t6.submtg_state_cd
    and t1.msis_ident_num = t6.msis_ident_num
    left join (
        select
            *
        from
            ELG00012_step2
        where
            keeper = 7
    ) t7 on t1.submtg_state_cd = t7.submtg_state_cd
    and t1.msis_ident_num = t7.msis_ident_num
    left join (
        select
            *
        from
            ELG00012_step2
        where
            keeper = 8
    ) t8 on t1.submtg_state_cd = t8.submtg_state_cd
    and t1.msis_ident_num = t8.msis_ident_num
    left join (
        select
            *
        from
            ELG00012_step2
        where
            keeper = 9
    ) t9 on t1.submtg_state_cd = t9.submtg_state_cd
    and t1.msis_ident_num = t9.msis_ident_num
    left join (
        select
            *
        from
            ELG00012_step2
        where
            keeper = 10
    ) t10 on t1.submtg_state_cd = t10.submtg_state_cd
    and t1.msis_ident_num = t10.msis_ident_num