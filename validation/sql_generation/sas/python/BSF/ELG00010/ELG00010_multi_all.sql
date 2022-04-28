create
or replace temporary view ELG00010_multi_all as
select
    t1.*,
    case
        when length(trim(mfp_prtcptn_endd_rsn_cd)) = 1
        and mfp_prtcptn_endd_rsn_cd <> '.' then lpad(mfp_prtcptn_endd_rsn_cd, 2, '0')
        else mfp_prtcptn_endd_rsn_cd
    end as mfp_prtcptn_endd_rsn_code,
    case
        when length(trim(mfp_qlfyd_instn_cd)) = 1
        and mfp_qlfyd_instn_cd <> '.' then lpad(mfp_qlfyd_instn_cd, 2, '0')
        else mfp_qlfyd_instn_cd
    end as mfp_qlfyd_instn_code,
    case
        when length(trim(mfp_qlfyd_rsdnc_cd)) = 1
        and mfp_qlfyd_rsdnc_cd <> '.' then lpad(mfp_qlfyd_rsdnc_cd, 2, '0')
        else mfp_qlfyd_rsdnc_cd
    end as mfp_qlfyd_rsdnc_code,
    case
        when length(trim(mfp_rinstlzd_rsn_cd)) = 1
        and mfp_rinstlzd_rsn_cd <> '.' then lpad(mfp_rinstlzd_rsn_cd, 2, '0')
        else mfp_rinstlzd_rsn_cd
    end as mfp_rinstlzd_rsn_code,
    1 as MFP_PARTICIPANT_FLG
from
    ELG00010 as t1
    inner join ELG00010_recCt as t2 on t1.submtg_state_cd = t2.submtg_state_cd
    and t1.msis_ident_num = t2.msis_ident_num
    and t2.recCt > 1