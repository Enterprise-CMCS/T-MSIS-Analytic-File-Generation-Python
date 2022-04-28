create temp table rx_hdr_rolled_0 as
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
            0 as inst_clms,
            0 as prof_clms,
            0 as dental_clms,
            0 dental_lne_cnts,
            0 as trnsprt_clms,
            0 as trnsprt_lne_cnts,
            0 as othr_hcbs_clms,
            0 as othr_hcbs_lne_cnts,
            0 as Lab_clms,
            0 as Lab_lne_cnts,
            0 as Rad_clms,
            0 as Rad_lne_cnts,
            0 as all_othr_inst_clms,
            0 as all_othr_prof_clms,
            0 as nf_clms,
            0 as inp_clms,
            0 as ic_clms,
            0 as othr_res_clms,
            0 as op_hosp_clms,
            0 as clinic_clms,
            0 as hospice_clms,
            0 as hh_clms
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
                    end as other_fin
                from
                    rx_lne_flag_tos_cat
                where
                    rec_cnt = 1
            ) a
    ) b