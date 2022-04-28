create temp table ELG00012_202111_uniq distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
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
    cast(null as varchar(20)) as WVR_ID6,
    cast(null as varchar(2)) as WVR_TYPE_CD6,
    cast(null as varchar(20)) as WVR_ID7,
    cast(null as varchar(2)) as WVR_TYPE_CD7,
    cast(null as varchar(20)) as WVR_ID8,
    cast(null as varchar(2)) as WVR_TYPE_CD8,
    cast(null as varchar(20)) as WVR_ID9,
    cast(null as varchar(2)) as WVR_TYPE_CD9,
    cast(null as varchar(20)) as WVR_ID10,
    cast(null as varchar(2)) as WVR_TYPE_CD10
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