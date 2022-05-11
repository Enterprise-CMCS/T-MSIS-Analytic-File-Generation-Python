create
or replace temporary view ELG00013_202111_uniq as
select
    t1.submtg_state_cd,
    t1.msis_ident_num,
    t1.LTSS_PRVDR_NUM as LTSS_PRVDR_NUM1,
    t1.LTSS_LVL_CARE_CD as LTSS_LVL_CARE_CD1,
    t2.LTSS_PRVDR_NUM as LTSS_PRVDR_NUM2,
    t2.LTSS_LVL_CARE_CD as LTSS_LVL_CARE_CD2,
    t3.LTSS_PRVDR_NUM as LTSS_PRVDR_NUM3,
    t3.LTSS_LVL_CARE_CD as LTSS_LVL_CARE_CD3
from
    (
        select
            *
        from
            ELG00013_step2
        where
            keeper = 1
    ) t1
    left join (
        select
            *
        from
            ELG00013_step2
        where
            keeper = 2
    ) t2 on t1.submtg_state_cd = t2.submtg_state_cd
    and t1.msis_ident_num = t2.msis_ident_num
    left join (
        select
            *
        from
            ELG00013_step2
        where
            keeper = 3
    ) t3 on t1.submtg_state_cd = t3.submtg_state_cd
    and t1.msis_ident_num = t3.msis_ident_num