create temp table ELG00009_202111_uniq sortkey(submtg_state_cd, msis_ident_num) as
select
    t1.submtg_state_cd,
    t1.msis_ident_num,
    t1.LCKIN_PRVDR_NUM as LCKIN_PRVDR_NUM1,
    t1.LCKIN_PRVDR_TYPE_CODE as LCKIN_PRVDR_TYPE_CD1,
    t2.LCKIN_PRVDR_NUM as LCKIN_PRVDR_NUM2,
    t2.LCKIN_PRVDR_TYPE_CODE as LCKIN_PRVDR_TYPE_CD2,
    t3.LCKIN_PRVDR_NUM as LCKIN_PRVDR_NUM3,
    t3.LCKIN_PRVDR_TYPE_CODE as LCKIN_PRVDR_TYPE_CD3,
case
        when LCKIN_PRVDR_NUM1 is not null
        or LCKIN_PRVDR_NUM2 is not null
        or LCKIN_PRVDR_NUM3 is not null then 1
        else 2
    end as LOCK_IN_FLG
from
    (
        select
            *
        from
            ELG00009_step2
        where
            keeper = 1
    ) t1
    left join (
        select
            *
        from
            ELG00009_step2
        where
            keeper = 2
    ) t2 on t1.submtg_state_cd = t2.submtg_state_cd
    and t1.msis_ident_num = t2.msis_ident_num
    left join (
        select
            *
        from
            ELG00009_step2
        where
            keeper = 3
    ) t3 on t1.submtg_state_cd = t3.submtg_state_cd
    and t1.msis_ident_num = t3.msis_ident_num