create temp table lt_hdr_rolled as
select
    *,
    case
        when cmc_php = 1 then '11'
        when other_pmpm = 1 then '12'
        when dsh_flag = 1 then '13'
        when other_fin = 1 then '14'
        when inp_clms = 1 then '21'
        when rx_clms = 1 then '41'
        when nf_clms = 1 then '22'
        when ic_clms = 1 then '23'
        when othr_res_clms = 1 then '24'
        when hospice_clms = 1 then '25'
        when rad_clms = 1 then '31'
        when lab_clms = 1 then '32'
        when hh_clms = 1 then '33'
        when trnsprt_clms = 1 then '34'
        when dental_clms = 1 then '35'
        when op_hosp_clms = 1 then '26'
        when clinic_clms = 1 then '27'
        when othr_hcbs_clms = 1 then '36'
        when dme_clms = 1 then '37'
        when all_othr_inst_clms = 1 then '28'
        when all_othr_prof_clms = 1 then '38'
    end as fed_srvc_ctgry_cd
from
    lt_hdr_rolled_0