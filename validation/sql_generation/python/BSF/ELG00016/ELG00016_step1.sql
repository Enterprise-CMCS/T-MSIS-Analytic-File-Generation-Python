create
or replace temporary view ELG00016_step1 as
select
    submtg_state_cd,
    msis_ident_num,
    CRTFD_AMRCN_INDN_ALSKN_NTV_IND,
    row_number() over (
        partition by submtg_state_cd,
        msis_ident_num
        order by
            submtg_state_cd,
            msis_ident_num,
            TMSIS_RPTG_PRD desc,
            RACE_DCLRTN_EFCTV_DT desc,
            RACE_DCLRTN_END_DT desc,
            REC_NUM desc,
            CRTFD_AMRCN_INDN_ALSKN_NTV_IND
    ) as keeper
from
    ELG00016
where
    CRTFD_AMRCN_INDN_ALSKN_NTV_IND is not null