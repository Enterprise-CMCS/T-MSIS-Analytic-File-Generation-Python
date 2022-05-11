create temp table ELG00002_death distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num, best_record) as
select
    submtg_state_cd,
    msis_ident_num,
    case
        when to_char(DEATH_DT, 'YYYYMM') :: integer > 202111 then null
        else DEATH_DT
    end as DEATH_DATE,
    case
        when DEATH_DT is not null
        and date_cmp(DEATH_DT, '30NOV2021') in(-1, 0) then 1
        else 0
    end as DECEASED_FLG,
    row_number() over (
        partition by submtg_state_cd,
        msis_ident_num
        order by
            submtg_state_cd,
            msis_ident_num,
            TMSIS_RPTG_PRD desc,
            PRMRY_DMGRPHC_ELE_EFCTV_DT desc,
            PRMRY_DMGRPHC_ELE_END_DT desc,
            REC_NUM desc,
            DEATH_DT desc
    ) as best_record
from
    ELG00002A