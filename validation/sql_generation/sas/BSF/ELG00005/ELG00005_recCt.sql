create temp table ELG00005_recCt distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num, recCt) as
select
    submtg_state_cd,
    msis_ident_num,
    count(TMSIS_RUN_ID) as recCt
from
    ELG00005
where
    PRMRY_ELGBLTY_GRP_IND = '1'
group by
    submtg_state_cd,
    msis_ident_num