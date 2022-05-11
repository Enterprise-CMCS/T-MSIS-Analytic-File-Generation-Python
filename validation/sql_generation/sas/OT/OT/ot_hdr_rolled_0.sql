create temp table ot_hdr_rolled_0 as
select
    b.*,
    inp_clms + rx_clms + ic_clms + nf_clms + othr_res_clms + dental_clms + trnsprt_clms + othr_hcbs_clms + op_hosp_clms + clinic_clms + hospice_clms + all_othr_inst_clms + lab_clms + rad_clms + hh_clms + dme_clms + all_othr_prof_clms as tot_num_srvc_flag,
    case
        when (
            inp_clms + rx_clms + ic_clms + nf_clms + othr_res_clms + dental_clms + trnsprt_clms + othr_hcbs_clms + op_hosp_clms + clinic_clms + hospice_clms + all_othr_inst_clms + lab_clms + rad_clms + hh_clms + dme_clms + all_othr_prof_clms
        ) = 0 then 0
        when (
            inp_clms + rx_clms + ic_clms + nf_clms + othr_res_clms + dental_clms + trnsprt_clms + othr_hcbs_clms + op_hosp_clms + clinic_clms + hospice_clms + all_othr_inst_clms + lab_clms + rad_clms + hh_clms + dme_clms + all_othr_prof_clms
        ) = 1 then 1
        when (
            inp_clms + rx_clms + ic_clms + nf_clms + othr_res_clms + dental_clms + trnsprt_clms + othr_hcbs_clms + op_hosp_clms + clinic_clms + hospice_clms + all_othr_inst_clms + lab_clms + rad_clms + hh_clms + dme_clms + all_othr_prof_clms
        ) > 1 then 2
        else null
    end as num_srvc_flag_grp,
    case
        when inst_clms = 0
        and prof_clms = 0 then 1
        else 0
    end as not_inst_prof
from
    (
        select
            a.*,
            cmc_php + other_pmpm + dsh_flag + other_fin as tot_num_fin_flag,
            0 as rx_clms,
            0 as nf_clms,
            0 as inp_clms,
            0 as ic_clms,
            0 as othr_res_clms
        from
            (
                select
                    *,
                    case
                        when (
                            srvc_trkg = 1
                            or supp_clms = 1
                            or cap_tos_null_toc = 1
                        ) then 1
                        else 0
                    end as other_fin,
                    case
                        when not_fin_clm = 1
                        and inst_clms = 1
                        and dental_clms = 0
                        and trnsprt_clms = 0
                        and othr_hcbs_clms = 0
                        and op_hosp_clms = 0
                        and clinic_clms = 0
                        and hospice_clms = 0 then 1
                        else 0
                    end as all_othr_inst_clms,
                    case
                        when not_fin_clm = 1
                        and prof_clms = 1
                        and dental_clms = 0
                        and trnsprt_clms = 0
                        and othr_hcbs_clms = 0
                        and Lab_clms = 0
                        and Rad_clms = 0
                        and hh_clms = 0
                        and DME_clms = 0 then 1
                        else 0
                    end as all_othr_prof_clms
                from
                    ot_lne_flag_tos_cat
                where
                    rec_cnt = 1
            ) a
    ) b