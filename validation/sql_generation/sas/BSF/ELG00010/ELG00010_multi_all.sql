create temp table ELG00010_multi_all distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    t1.*,
    lpad(trim(mfp_prtcptn_endd_rsn_cd), 2, '0') as mfp_prtcptn_endd_rsn_code,
    lpad(trim(mfp_qlfyd_instn_cd), 2, '0') as mfp_qlfyd_instn_code,
    lpad(trim(mfp_qlfyd_rsdnc_cd), 2, '0') as mfp_qlfyd_rsdnc_code,
    lpad(trim(mfp_rinstlzd_rsn_cd), 2, '0') as mfp_rinstlzd_rsn_code,
    1 as MFP_PARTICIPANT_FLG
from
    ELG00010 t1
    inner join ELG00010_recCt t2 on t1.submtg_state_cd = t2.submtg_state_cd
    and t1.msis_ident_num = t2.msis_ident_num
    and t2.recCt > 1