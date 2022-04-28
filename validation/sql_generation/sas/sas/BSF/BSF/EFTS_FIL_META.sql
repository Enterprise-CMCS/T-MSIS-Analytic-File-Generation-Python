INSERT INTO
    data_anltcs_dm_prod.EFTS_FIL_META (
        da_run_id,
        fil_4th_node_txt,
        otpt_name,
        rptg_prd,
        itrtn_num,
        tot_rec_cnt,
        fil_cret_dt,
        incldd_state_cd,
        rec_cnt_by_state_cd,
        fil_dt,
        taf_cd_spec_vrsn_name
    )
SELECT
    t1.da_run_id,
    t2.fil_4th_node_txt,
    t2.otpt_name,
    to_char(
        cast(substring(t1.job_parms_txt, 1, 10) as date),
        'Month,YYYY'
    ) as rptg_prd,
    substring(t1.taf_cd_spec_vrsn_name, 2, 2) as itrtn_num,
    t2.rec_cnt as tot_rec_cnt,
    to_char(date(t2.rec_add_ts), 'MM/DD/YYYY') as fil_cret_dt,
    coalesce(t3.submtg_state_cd, 'Missing') as incldd_state_cd,
    t3.audt_cnt_val as rec_cnt_by_state_cd,
    '202111' as fil_dt,
    t1.taf_cd_spec_vrsn_name
from
    data_anltcs_dm_prod.job_cntl_parms t1,
    data_anltcs_dm_prod.job_otpt_meta t2,
    data_anltcs_dm_prod.pgm_audt_cnts t3,
    data_anltcs_dm_prod.pgm_audt_cnt_lkp t4
where
    t1.da_run_id = 6194
    and t1.da_run_id = t2.da_run_id
    and t2.da_run_id = t3.da_run_id
    and t2.otpt_name = 'TAF_MON_BSF'
    and t3.pgm_audt_cnt_id = t4.pgm_audt_cnt_id
    and t1.sucsfl_ind = true
    and t4.pgm_name = '023_bsf_ELG00023'
    and t4.step_name = '0.1. create_initial_table'
    and t4.obj_name = 'BSF_&RPT_OUT._&BSF_FILE_DATE'
    and t4.audt_cnt_of = 'submtg_state_cd'