create temp table ELG00004A_recCt distkey(msis_ident_num) sortkey(submtg_state_cd,msis_ident_num) as select submtg_state_cd, msis_ident_num, count(msis_ident_num) as recCt from ELG00004 where ELGBL_ADR_TYPE_CD in ('06','6') group by submtg_state_cd, 
msis_ident_num
