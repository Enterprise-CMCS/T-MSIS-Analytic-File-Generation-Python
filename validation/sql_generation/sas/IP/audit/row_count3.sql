create temp table row_count3 as
select
    da_run_id,
    pgm_audt_cnt_id,
    submtg_state_cd,
    audt_cnt_val
from
    (
        select
            6000 as da_run_id,
            617 as pgm_audt_cnt_id,
            t1.state as submtg_state_cd,
            t1.cnt as audt_cnt_val
        from
            (
                select
                    new_submtg_state_cd_line as state,
                    count(new_submtg_state_cd_line) as cnt
                from
                    (
                        select
                            distinct orgnl_clm_num_line,
                            adjstmt_clm_num_line,
                            adjdctn_dt_line,
                            line_adjstmt_ind,
                            orgnl_line_num,
                            adjstmt_line_num,
                            new_submtg_state_cd_line
                        from
                            IP_LINE
                    ) as t0
                group by
                    new_submtg_state_cd_line
            ) as t1
    ) as t2
union
select
    6000 as da_run_id,
    617 as pgm_audt_cnt_id,
    'xx' as submtg_state_cd,
    0 as audt_cnt_val