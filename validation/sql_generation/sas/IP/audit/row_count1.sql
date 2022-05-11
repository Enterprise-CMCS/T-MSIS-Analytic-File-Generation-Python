create temp table row_count1 as
select
    da_run_id,
    pgm_audt_cnt_id,
    submtg_state_cd,
    audt_cnt_val
from
    (
        select
            6000 as da_run_id,
            598 as pgm_audt_cnt_id,
            t1.state as submtg_state_cd,
            t1.cnt as audt_cnt_val
        from
            (
                select
                    submtg_state_cd as state,
                    count(submtg_state_cd) as cnt
                from
                    (
                        select
                            distinct orgnl_clm_num,
                            adjstmt_clm_num,
                            adjdctn_dt,
                            adjstmt_ind,
                            submtg_state_cd
                        from
                            FA_HDR_IP
                    ) as t0
                group by
                    submtg_state_cd
            ) as t1
    ) as t2
union
select
    6000 as da_run_id,
    598 as pgm_audt_cnt_id,
    'xx' as submtg_state_cd,
    0 as audt_cnt_val
