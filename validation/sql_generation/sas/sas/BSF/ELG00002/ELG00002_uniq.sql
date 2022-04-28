create temp table ELG00002_uniq distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    t1.*,
    upper(GNDR_CD) as GNDR_CODE,
    1 as KEEP_FLAG
from
    ELG00002 t1
    inner join ELG00002_recCt t2 on t1.submtg_state_cd = t2.submtg_state_cd
    and t1.msis_ident_num = t2.msis_ident_num
    and t2.recCt = 1
