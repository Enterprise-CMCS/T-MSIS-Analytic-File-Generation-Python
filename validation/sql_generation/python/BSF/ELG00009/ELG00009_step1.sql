create
or replace temporary view ELG00009_step1 as
select
    distinct submtg_state_cd,
    msis_ident_num,
    lckin_prvdr_num,
    TMSIS_RPTG_PRD,
    LCKIN_EFCTV_DT,
    LCKIN_END_DT,
    REC_NUM,
    lpad(lckin_prvdr_type_cd, 2, '0') as lckin_prvdr_type_code,
    row_number() over (
        partition by submtg_state_cd,
        msis_ident_num,
        lckin_prvdr_num,
        lpad(lckin_prvdr_type_cd, 2, '0')
        order by
            submtg_state_cd,
            msis_ident_num,
            TMSIS_RPTG_PRD desc,
            LCKIN_EFCTV_DT desc,
            LCKIN_END_DT desc,
            REC_NUM desc,
            lckin_prvdr_num,
            lpad(lckin_prvdr_type_cd, 2, '0')
    ) as lckin_deduper
from
    (
        select
            *
        from
            ELG00009
        where
            lckin_prvdr_num is not null
            or lckin_prvdr_type_cd is not null
    ) t1